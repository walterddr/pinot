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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.logical.RexExpression;
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
import org.apache.pinot.query.planner.stage.WindowNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
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
      TimeBoundaryInfo timeBoundaryInfo, TableType tableType, List<String> segmentList,
      TransferableBlock dynamicMailboxResultBlock) {
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
            timeBoundaryInfo, dynamicMailboxResultBlock);

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
  public Void visitWindow(WindowNode node, ServerPlanRequestContext context) {
    throw new UnsupportedOperationException("Window not yet supported!");
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
    TransferableBlock block = context.getDynamicOperatorResult();

    // step 2: write filter expression (both SEMI and INNER join)
    //   1. join keys will be rewritten as IN clause
    //   2. inequality joins will be rewritten as min/max range filter (TODO: not implemented yet)
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
      attachLocalJoin(context.getPinotQuery(), node);
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
    List<Object[]> container = block.getContainer();
    int numRows = block.getNumRows();
    switch (storedType) {
      case INT:
        int[] arrInt = new int[block.getNumRows()];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrInt[rowIdx] = (int) container.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrInt);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrInt[rowIdx]));
        }
        break;
      case LONG:
        long[] arrLong = new long[block.getNumRows()];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrLong[rowIdx] = (long) container.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrLong);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrLong[rowIdx]));
        }
        break;
      case FLOAT:
        float[] arrFloat = new float[block.getNumRows()];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrFloat[rowIdx] = (float) container.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrFloat);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrFloat[rowIdx]));
        }
        break;
      case DOUBLE:
        double[] arrDouble = new double[block.getNumRows()];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrDouble[rowIdx] = (double) container.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrDouble);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrDouble[rowIdx]));
        }
        break;
      case STRING:
        String[] arrString = new String[block.getNumRows()];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrString[rowIdx] = (String) container.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrString);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrString[rowIdx]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
    }
    return expressions;
  }

  /**
   * Attach Local Join clause
   */
  private static void attachLocalJoin(PinotQuery pinotQuery, JoinNode joinNode) {
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
    List<RexExpression> joinClauses = joinNode.getJoinClauses();
    List<String> filterStrs = new ArrayList<>(joinClauses.size());
    Set<String> leftFilterExpr = new HashSet<>();
    Set<String> rightFilterExpr = new HashSet<>();
    for (RexExpression joinClause : joinClauses) {
      // TODO: assumption: Always function call
      Preconditions.checkState(joinClause instanceof RexExpression.FunctionCall, "only support function calls");
      RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) joinClause;
      String functionName = functionCall.getFunctionName();
      // TODO: assumption: Always bilateral operator
      Preconditions.checkState(functionCall.getFunctionOperands().size() == 2, "only support bi-ops");
      List<RexExpression> ops = functionCall.getFunctionOperands();
      // TODO: assumption: Always left then right
      // TODO: assumption: no additional computation
      int leftIdx = ((RexExpression.InputRef) ops.get(0)).getIndex();
      int rightIdx = ((RexExpression.InputRef) ops.get(1)).getIndex();
      String leftOpStr = leftSchema.get(leftIdx);
      String rightOpStr = rightSchema.get(rightIdx - leftSchema.size());
      leftFilterExpr.add(leftOpStr);
      rightFilterExpr.add(rightOpStr);
      leftOpStr = "$L." + leftOpStr;
      rightOpStr = "$R." + rightOpStr;
      switch (functionName) {
        case "equals":
        case "=":
          filterStrs.add(leftOpStr + " = " + rightOpStr);
          break;
        case "notEquals":
        case "!=":
        case "<>":
          filterStrs.add(leftOpStr + " != " + rightOpStr);
          break;
        case "greaterThan":
        case ">":
          filterStrs.add(leftOpStr + " > " + rightOpStr);
          break;
        case "greaterThanOrEqual":
        case ">=":
          filterStrs.add(leftOpStr + " >= " + rightOpStr);
          break;
        case "lessThan":
        case "<":
          filterStrs.add(leftOpStr + " < " + rightOpStr);
          break;
        case "lessThanOrEqual":
        case "<=":
          filterStrs.add(leftOpStr + " <= " + rightOpStr);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported join filter");
      }
    }

    // 3. attach selection columns
    List<String> projectLeft = leftSchema;
    List<String> projectRight = rightSchema;

    // construct the final expression
    Expression functionExpression = createFunction("localjoin", Arrays.asList(
        createFunction("leftJoinKeys",
            Collections.singletonList(RequestUtils.getIdentifierExpression(joinKeyLeft.get(0)))),
        createFunction("rightJoinKeys",
            Collections.singletonList(RequestUtils.getIdentifierExpression(joinKeyRight.get(0)))),
        createFunction("leftFilterColumns",
            leftFilterExpr.stream().map(RequestUtils::getIdentifierExpression).collect(Collectors.toList())),
        createFunction("rightFilterColumns",
            rightFilterExpr.stream().map(RequestUtils::getIdentifierExpression).collect(Collectors.toList())),
        RequestUtils.getLiteralExpression(StringUtils.join(filterStrs, " AND ")),
        createFunction("leftProjectColumns",
            projectLeft.stream().map(RequestUtils::getIdentifierExpression).collect(Collectors.toList())),
        createFunction("rightProjectColumns",
            projectRight.stream().map(RequestUtils::getIdentifierExpression).collect(Collectors.toList()))
    ));
    pinotQuery.setSelectList(Collections.singletonList(functionExpression));
  }

  private static Expression createFunction(String functionName, List<Expression> operands) {
    Expression functionExpression = new Expression(ExpressionType.FUNCTION);
    Function function = new Function(functionName);
    function.setOperands(operands);
    functionExpression.setFunctionCall(function);
    return functionExpression;
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
}
