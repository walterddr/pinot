/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.plan;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Plan visitor for direct leaf-stage server request.
 *
 * This should be merged with logics in {@link org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2} in the future
 * to directly produce operator chain.
 *
 * As of now, the reason why we use the plan visitor for server request is for additional support such as dynamic
 * filtering and other auxiliary functionalities.
 */
public class ServerRequestPlanVisitor implements StageNodeVisitor<Void, ServerPlanRequestContext> {
  private static final int DEFAULT_LEAF_NODE_LIMIT = 10_000_000;
  private static final String DYNAMIC_TABLE_NAME = "DynamicTable";
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerRequestPlanVisitor.class);
  private static final List<String> QUERY_REWRITERS_CLASS_NAMES =
      ImmutableList.of(PredicateComparisonRewriter.class.getName(),
          NonAggregationGroupByToDistinctQueryRewriter.class.getName());
  private static final List<QueryRewriter> QUERY_REWRITERS =
      new ArrayList<>(QueryRewriterFactory.getQueryRewriters(QUERY_REWRITERS_CLASS_NAMES));
  private static final QueryOptimizer QUERY_OPTIMIZER = new QueryOptimizer();

  private static final ServerRequestPlanVisitor INSTANCE = new ServerRequestPlanVisitor();
  private static Void _aVoid = null;

  public static ServerPlanRequestContext build(MailboxService<TransferableBlock> mailboxService,
      DistributedStagePlan stagePlan, Map<String, String> requestMetadataMap, TableConfig tableConfig, Schema schema,
      TimeBoundaryInfo timeBoundaryInfo, TableType tableType, List<String> segmentList) {
    // Before-visit: construct the ServerPlanRequestContext baseline
    long requestId = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID));
    long timeoutMs = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS));
    PinotQuery pinotQuery = new PinotQuery();
    Integer leafNodeLimit = QueryOptionsUtils.getMultiStageLeafLimit(requestMetadataMap);
    if (leafNodeLimit != null) {
      pinotQuery.setLimit(leafNodeLimit);
    } else {
      pinotQuery.setLimit(DEFAULT_LEAF_NODE_LIMIT);
    }
    LOGGER.debug("QueryID" + requestId + " leafNodeLimit:" + leafNodeLimit);
    // 1. set PinotQuery default according to requestMetadataMap
    pinotQuery.setExplain(false);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, String.valueOf(timeoutMs));
    pinotQuery.setQueryOptions(queryOptions);

    // 2. visit the plan and create query physical plan.
    ServerPlanRequestContext context =
        new ServerPlanRequestContext(mailboxService, requestId, stagePlan.getStageId(), timeoutMs,
            new VirtualServerAddress(stagePlan.getServer()), stagePlan.getMetadataMap(), pinotQuery, tableType,
            timeBoundaryInfo);

    ServerRequestPlanVisitor.walkStageNode(stagePlan.getStageRoot(), context);

    // Post-visit: finalize context.
    // 3. global rewrite/optimize
    if (timeBoundaryInfo != null) {
      attachTimeBoundary(pinotQuery, timeBoundaryInfo, tableType == TableType.OFFLINE);
    }
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    QUERY_OPTIMIZER.optimize(pinotQuery, tableConfig, schema);

    // 4. wrapped around in broker request
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    DataSource dataSource = pinotQuery.getDataSource();
    if (dataSource != null) {
      QuerySource querySource = new QuerySource();
      querySource.setTableName(dataSource.getTableName());
      brokerRequest.setQuerySource(querySource);
    }

    // 5. create instance request with segmentList
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setBrokerId("unknown");
    instanceRequest.setEnableTrace(false);
    instanceRequest.setSearchSegments(segmentList);
    instanceRequest.setQuery(brokerRequest);

    context.setInstanceRequest(instanceRequest);
    return context;
  }

  private static void walkStageNode(StageNode node, ServerPlanRequestContext context) {
    node.visit(INSTANCE, context);
  }

  @Override
  public Void visitAggregate(AggregateNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    // set group-by list
    context.getPinotQuery()
        .setGroupByList(CalciteRexExpressionParser.convertGroupByList(node.getGroupSet(), context.getPinotQuery()));
    // set agg list
    context.getPinotQuery().setSelectList(
        CalciteRexExpressionParser.addSelectList(context.getPinotQuery().getGroupByList(), node.getAggCalls(),
            context.getPinotQuery()));
    return _aVoid;
  }

  @Override
  public Void visitFilter(FilterNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    context.getPinotQuery()
        .setFilterExpression(CalciteRexExpressionParser.toExpression(node.getCondition(), context.getPinotQuery()));
    return _aVoid;
  }

  @Override
  public Void visitJoin(JoinNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    // visit only the static side.
    StageNode staticSide = node.getInputs().get(0);
    StageNode dynamicSide = node.getInputs().get(1);
    if (staticSide instanceof MailboxReceiveNode) {
      dynamicSide = node.getInputs().get(0);
      staticSide = node.getInputs().get(1);
    }
    staticSide.visit(this, context);

    // rewrite context for JOIN as dynamic filter
    // step 1: get the other-side's result.
    MailboxReceiveNode dynamicMailbox = (MailboxReceiveNode) dynamicSide;
    List<VirtualServer> sendingInstances = context.getMetadataMap().get(dynamicMailbox.getSenderStageId())
        .getServerInstances();
    MailboxReceiveOperator mailboxReceiveOperator = new MailboxReceiveOperator(context.getMailboxService(),
        sendingInstances, dynamicMailbox.getExchangeType(), context.getServer(),
        context.getRequestId(), dynamicMailbox.getSenderStageId(), context.getTimeoutMs());
    TransferableBlock block = receiveDataBlock(mailboxReceiveOperator, dynamicMailbox.getDataSchema(),
        context.getTimeoutMs());

    // step 2: write filter expression
    //   1. join keys will be rewritten as either IN clause or IN_IDSET
    //   2. inequality joins will be rewritten as min/max range filter
    context.setDynamicOperatorResult(block);
    if (block.getNumRows() > 0) {
      attachDynamicFilter(context.getPinotQuery(), node.getJoinKeys(), block);
    } else {
      // filter is empty, we can return a constant empty block.
      context.addHints(ServerPlanRequestContext.EMPTY_RETURN_HINT);
    }

    // step 3: write project expression
    //   1. equality join conditions will be used as join key
    //   2. inequality join conditions will be rewritten as local join filter
    //   3. projects will be rewritten as local project columns
    if (node.getJoinRelType() == JoinRelType.INNER) {
      attachJoinTable(context.getPinotQuery(), node, block);
    }
    return _aVoid;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    return _aVoid;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    return _aVoid;
  }

  @Override
  public Void visitProject(ProjectNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    context.getPinotQuery()
        .setSelectList(CalciteRexExpressionParser.overwriteSelectList(node.getProjects(), context.getPinotQuery()));
    return _aVoid;
  }

  @Override
  public Void visitSort(SortNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    if (node.getCollationKeys().size() > 0) {
      context.getPinotQuery().setOrderByList(
          CalciteRexExpressionParser.convertOrderByList(node.getCollationKeys(), node.getCollationDirections(),
              context.getPinotQuery()));
    }
    if (node.getFetch() > 0) {
      context.getPinotQuery().setLimit(node.getFetch());
    }
    if (node.getOffset() > 0) {
      context.getPinotQuery().setOffset(node.getOffset());
    }
    return _aVoid;
  }

  @Override
  public Void visitTableScan(TableScanNode node, ServerPlanRequestContext context) {
    DataSource dataSource = new DataSource();
    String tableNameWithType = TableNameBuilder.forType(context.getTableType())
        .tableNameWithType(TableNameBuilder.extractRawTableName(node.getTableName()));
    dataSource.setTableName(tableNameWithType);
    context.getPinotQuery().setDataSource(dataSource);
    context.getPinotQuery().setSelectList(
        node.getTableScanColumns().stream().map(RequestUtils::getIdentifierExpression).collect(Collectors.toList()));
    return _aVoid;
  }

  @Override
  public Void visitValue(ValueNode node, ServerPlanRequestContext context) {
    visitChildren(node, context);
    return _aVoid;
  }

  private void visitChildren(StageNode node, ServerPlanRequestContext context) {
    for (StageNode child : node.getInputs()) {
      child.visit(this, context);
    }
  }


  /**
   * attach the dynamic filter to the given PinotQuery.
   */
  private static void attachDynamicFilter(PinotQuery pinotQuery, JoinNode.JoinKeys joinKeys,
      TransferableBlock dataBlock) {
    FieldSelectionKeySelector leftSelector = (FieldSelectionKeySelector) joinKeys.getLeftJoinKeySelector();
    FieldSelectionKeySelector rightSelector = (FieldSelectionKeySelector) joinKeys.getRightJoinKeySelector();
    List<Expression> expressions = new ArrayList<>();
    for (int i = 0; i < leftSelector.getColumnIndices().size(); i++) {
      Expression leftExpr = pinotQuery.getSelectList().get(leftSelector.getColumnIndices().get(i));
      int rightIdx = rightSelector.getColumnIndices().get(i);
      Expression inFilterExpr = RequestUtils.getFunctionExpression(FilterKind.IN.name());
      List<Expression> operands = new ArrayList<>(dataBlock.getNumRows() + 1);
      operands.add(leftExpr);
      operands.addAll(computeInOperands(dataBlock, rightIdx));
      inFilterExpr.getFunctionCall().setOperands(operands);
      expressions.add(inFilterExpr);
    }
    attachFilterExpression(pinotQuery, FilterKind.AND, expressions);
  }

  private static List<Expression> computeInOperands(TransferableBlock block, int colIdx) {
    final DataSchema.ColumnDataType columnDataType = block.getDataSchema().getColumnDataType(colIdx);
    final FieldSpec.DataType storedType = columnDataType.getStoredType().toDataType();;
    List<Expression> expressions = new ArrayList<>(block.getNumRows());
    for (Object[] row : block.getContainer()) {
      switch (storedType) {
        case INT:
          expressions.add(RequestUtils.getLiteralExpression((int) row[colIdx]));
          break;
        case LONG:
          expressions.add(RequestUtils.getLiteralExpression((long) row[colIdx]));
          break;
        case FLOAT:
          expressions.add(RequestUtils.getLiteralExpression((float) row[colIdx]));
          break;
        case DOUBLE:
          expressions.add(RequestUtils.getLiteralExpression((double) row[colIdx]));
          break;
        case STRING:
          expressions.add(RequestUtils.getLiteralExpression((String) row[colIdx]));
          break;
        default:
          throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
      }
    }
    return expressions;
  }

  /**
   * Attach Local Join clause
   */
  private static void attachJoinTable(PinotQuery pinotQuery, JoinNode joinNode, TransferableBlock block) {
    List<String> leftSchema = joinNode.getLeftColumnNames();
    List<String> rightSchema = joinNode.getRightColumnNames();

    // 1. attach equality join key
    JoinNode.JoinKeys joinKeys = joinNode.getJoinKeys();
    FieldSelectionKeySelector leftSelector = (FieldSelectionKeySelector) joinKeys.getLeftJoinKeySelector();
    FieldSelectionKeySelector rightSelector = (FieldSelectionKeySelector) joinKeys.getRightJoinKeySelector();
    List<String> joinKeyLeft = new ArrayList<>(leftSelector.getColumnIndices().size());
    List<String> joinKeyRight = new ArrayList<>(rightSelector.getColumnIndices().size());
    for (int i = 0; i < leftSelector.getColumnIndices().size(); i++) {
      joinKeyLeft.add(leftSchema.get(leftSelector.getColumnIndices().get(i)));
      joinKeyRight.add(rightSchema.get(rightSelector.getColumnIndices().get(i)));
    }
    // 2. attach filter clause
    // TODO: add attach filter

    // 3. attach selection columns
    List<String> projectLeft = leftSchema;
    List<String> projectRight = rightSchema;

    // 4. attach data table
    attachInmemoryTable(pinotQuery, block);

    // construct the final expression
    Expression functionExpression = new Expression(ExpressionType.FUNCTION);
    Function function = new Function("JoinTable");
    functionExpression.setFunctionCall(function);
    List<Expression> operands = new ArrayList<>(8);
    operands.add(RequestUtils.getLiteralExpression(StringUtils.join(joinKeyLeft, ',')));
    operands.add(RequestUtils.getLiteralExpression(DYNAMIC_TABLE_NAME));
    operands.add(RequestUtils.getLiteralExpression(StringUtils.join(joinKeyRight, ',')));
    operands.add(RequestUtils.getLiteralExpression(""));
    operands.add(RequestUtils.getLiteralExpression(""));
    operands.add(RequestUtils.getLiteralExpression(""));
    operands.add(RequestUtils.getLiteralExpression(StringUtils.join(projectLeft, ',')));
    operands.add(RequestUtils.getLiteralExpression(StringUtils.join(projectRight, ',')));
    functionExpression.getFunctionCall().setOperands(operands);
    pinotQuery.setSelectList(Collections.singletonList(functionExpression));
  }

  private static void attachInmemoryTable(PinotQuery pinotQuery, TransferableBlock block) {
    List<Object[]> rows = block.getContainer();
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(block.getDataSchema());
    DataSchema.ColumnDataType[] dataTypes = block.getDataSchema().getColumnDataTypes();
    try {
      for (int rowId = 0; rowId < rows.size(); rowId++) {
        dataTableBuilder.startRow();
        Object[] row = rows.get(rowId);
        //TODO: Handle other types. Assume it is all string types.
        for (int colId = 0; colId < row.length; colId++) {
          switch (dataTypes[colId]) {
            case STRING:
              dataTableBuilder.setColumn(colId, (String) row[colId]);
              break;
            case BOOLEAN: // fall through
            case INT:
              dataTableBuilder.setColumn(colId, ((Integer) row[colId]).intValue());
              break;
            case LONG:
              dataTableBuilder.setColumn(colId, ((Long) row[colId]).longValue());
              break;
            case DOUBLE:
              dataTableBuilder.setColumn(colId, ((Double) row[colId]).doubleValue());
              break;
            case FLOAT:
              dataTableBuilder.setColumn(colId, ((Float) row[colId]).floatValue());
              break;
            default:
              throw new RuntimeException("Unsupported data type:" + dataTypes[colId]);
          }
        }
        dataTableBuilder.finishRow();
      }
      DataTable dataTable = dataTableBuilder.build();
      String dataTableStr = Base64.getEncoder().encodeToString(dataTable.toBytes());
      Map<String, String> queryOptions = pinotQuery.getQueryOptions();
      queryOptions = queryOptions == null ? new HashMap<>() : queryOptions;
      queryOptions.put(QueryOptionsUtils.IN_MEMORY_TABLE_DATATABLE_SUFFIX + DYNAMIC_TABLE_NAME, dataTableStr);
      pinotQuery.setQueryOptions(queryOptions);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Attach the time boundary to the given PinotQuery.
   */
  private static void attachTimeBoundary(PinotQuery pinotQuery, TimeBoundaryInfo timeBoundaryInfo,
      boolean isOfflineRequest) {
    String timeColumn = timeBoundaryInfo.getTimeColumn();
    String timeValue = timeBoundaryInfo.getTimeValue();
    Expression timeFilterExpression = RequestUtils.getFunctionExpression(
        isOfflineRequest ? FilterKind.LESS_THAN_OR_EQUAL.name() : FilterKind.GREATER_THAN.name());
    timeFilterExpression.getFunctionCall().setOperands(
        Arrays.asList(RequestUtils.getIdentifierExpression(timeColumn), RequestUtils.getLiteralExpression(timeValue)));

    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      Expression andFilterExpression = RequestUtils.getFunctionExpression(FilterKind.AND.name());
      andFilterExpression.getFunctionCall().setOperands(Arrays.asList(filterExpression, timeFilterExpression));
      pinotQuery.setFilterExpression(andFilterExpression);
    } else {
      pinotQuery.setFilterExpression(timeFilterExpression);
    }

    attachFilterExpression(pinotQuery, FilterKind.AND, Collections.singletonList(timeFilterExpression));
  }

  /**
   * Attach Filter Expression
   */
  private static void attachFilterExpression(PinotQuery pinotQuery, FilterKind attachKind, List<Expression> exprs) {
    Preconditions.checkState(attachKind == FilterKind.AND || attachKind == FilterKind.OR);
    Expression filterExpression = pinotQuery.getFilterExpression();
    List<Expression> arrayList = new ArrayList<>(exprs);
    if (filterExpression != null) {
      arrayList.add(filterExpression);
    }
    if (arrayList.size() > 1) {
      Expression attachFilterExpression = RequestUtils.getFunctionExpression(attachKind.name());
      attachFilterExpression.getFunctionCall().setOperands(arrayList);
      pinotQuery.setFilterExpression(attachFilterExpression);
    } else {
      pinotQuery.setFilterExpression(arrayList.get(0));
    }
  }

  /**
   * Compute dynamic operator block received from a chain operators
   */
  private static TransferableBlock receiveDataBlock(MultiStageOperator baseOperator,
      DataSchema dataSchema, long timeoutMs) {
  long timeoutWatermark = System.nanoTime() + timeoutMs * 1_000_000L;
    List<Object[]> dataContainer = new ArrayList<>();
    while (System.nanoTime() < timeoutWatermark) {
      TransferableBlock transferableBlock = baseOperator.nextBlock();
      if (TransferableBlockUtils.isEndOfStream(transferableBlock) && transferableBlock.isErrorBlock()) {
        // TODO: we only received bubble up error from the execution stage tree.
        // TODO: query dispatch should also send cancel signal to the rest of the execution stage tree.
        throw new RuntimeException(
            "Received error query execution result block: " + transferableBlock.getDataBlock().getExceptions());
      }
      if (transferableBlock.isNoOpBlock()) {
        continue;
      } else if (transferableBlock.isEndOfStreamBlock()) {
        return new TransferableBlock(dataContainer, dataSchema, BaseDataBlock.Type.ROW);
      }
      dataContainer.addAll(transferableBlock.getContainer());
    }
    return new TransferableBlock(dataContainer, dataSchema, BaseDataBlock.Type.ROW);
  }
}
