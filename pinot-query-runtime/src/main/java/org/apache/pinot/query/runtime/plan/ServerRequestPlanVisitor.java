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
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
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
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
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
    pinotQuery.setExplain(false);
    ServerPlanRequestContext context =
        new ServerPlanRequestContext(mailboxService, requestId, stagePlan.getStageId(), timeoutMs,
            stagePlan.getServerInstance().getHostname(), stagePlan.getServerInstance().getPort(),
            stagePlan.getMetadataMap(), pinotQuery, tableType, timeBoundaryInfo);

    // visit the plan and create query physical plan.
    ServerRequestPlanVisitor.walkStageNode(stagePlan.getStageRoot(), context);

    // Post-visit: finalize context.
    // 1. global rewrite/optimize
    if (timeBoundaryInfo != null) {
      attachTimeBoundary(pinotQuery, timeBoundaryInfo, tableType == TableType.OFFLINE);
    }
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    QUERY_OPTIMIZER.optimize(pinotQuery, tableConfig, schema);

    // 2. set pinot query options according to requestMetadataMap
    pinotQuery.setQueryOptions(
        ImmutableMap.of(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, String.valueOf(timeoutMs)));

    // 3. wrapped around in broker request
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    DataSource dataSource = pinotQuery.getDataSource();
    if (dataSource != null) {
      QuerySource querySource = new QuerySource();
      querySource.setTableName(dataSource.getTableName());
      brokerRequest.setQuerySource(querySource);
    }

    // 3. create instance request with segmentList
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
    List<ServerInstance> sendingInstances = context.getMetadataMap().get(dynamicMailbox.getSenderStageId())
        .getServerInstances();
    MailboxReceiveOperator mailboxReceiveOperator = new MailboxReceiveOperator(context.getMailboxService(),
         sendingInstances, dynamicMailbox.getExchangeType(), context.getHostName(), context.getPort(),
        context.getRequestId(), dynamicMailbox.getSenderStageId(), context.getTimeoutMs());
    TransferableBlock block = attachDynamicOperatorResults(mailboxReceiveOperator, dynamicMailbox.getDataSchema());
    context.setDynamicOperatorResult(block);
    // step 2: write filter expression
    attachDynamicFilter(context.getPinotQuery(), node.getJoinKeys(), block);
    // step 3: write project expression
    // TODO: write dynamic project
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
   * Helper method to attach the dynamic filter to the given PinotQuery.
   */
  private static void attachDynamicFilter(PinotQuery pinotQuery, JoinNode.JoinKeys joinKeys,
      TransferableBlock dataBlock) {
    FieldSelectionKeySelector leftSelector = (FieldSelectionKeySelector) joinKeys.getLeftJoinKeySelector();
    FieldSelectionKeySelector rightSelector = (FieldSelectionKeySelector) joinKeys.getRightJoinKeySelector();
    List<Expression> expressions = new ArrayList<>();
    for (int i = 0; i < leftSelector.getColumnIndices().size(); i++) {
      Expression leftExpr = pinotQuery.getSelectList().get(leftSelector.getColumnIndices().get(i));
      int rightIdx = rightSelector.getColumnIndices().get(i);
      Expression dynamicFilter = RequestUtils.getFunctionExpression(TransformFunctionType.INIDSET.name());
      dynamicFilter.getFunctionCall().setOperands(Arrays.asList(leftExpr, computeIdSet(dataBlock, rightIdx)));
      Expression filterExpr = RequestUtils.getFunctionExpression(FilterKind.EQUALS.name());
      filterExpr.getFunctionCall().setOperands(Arrays.asList(dynamicFilter, RequestUtils.getLiteralExpression(true)));
      expressions.add(filterExpr);
    }
    attachFilterExpression(pinotQuery, FilterKind.AND, expressions);
  }

  private static Expression computeIdSet(TransferableBlock block, int idx) {
    final DataSchema.ColumnDataType columnDataType = block.getDataSchema().getColumnDataType(idx);
    final FieldSpec.DataType storedType = columnDataType.getStoredType().toDataType();;
    IdSet idSet = IdSets.create(columnDataType.toDataType());
    for (Object[] row : block.getContainer()) {
      switch (storedType) {
        case INT:
          idSet.add((int) row[idx]);
          break;
        case LONG:
          idSet.add((long) row[idx]);
          break;
        case FLOAT:
          idSet.add((float) row[idx]);
          break;
        case DOUBLE:
          idSet.add((double) row[idx]);
          break;
        case STRING:
          idSet.add((String) row[idx]);
          break;
        default:
          throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
      }
    }
    try {
      return RequestUtils.getLiteralExpression(idSet.toBase64String());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Helper method to attach the time boundary to the given PinotQuery.
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

  private static TransferableBlock attachDynamicOperatorResults(BaseOperator<TransferableBlock> baseOperator,
      DataSchema dataSchema) {
    TransferableBlock mergedBlock = new TransferableBlock(new ArrayList<>(), dataSchema, BaseDataBlock.Type.ROW);
    TransferableBlock block = baseOperator.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(block)) {
      mergedBlock.getContainer().addAll(block.getContainer());
      block = baseOperator.nextBlock();
    }
    return mergedBlock;
  }
}
