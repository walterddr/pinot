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
package org.apache.pinot.core.plan;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ReplicatedJoinOperator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>LocalJoinPlanNode</code> class provides the execution plan for join only query
 * select JOIN_TABLE(leftJoinKey, rightTableName, rightJoinKey, leftFilterColumns, rightFilterColumns,
 * filterExpression, rightProjectColumns, leftProjectColumns)
 */
@SuppressWarnings("rawtypes")
public class LocalJoinPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalJoinPlanNode.class);

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public LocalJoinPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<SelectionResultsBlock> run() {
    int numTotalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    List<ExpressionContext> selectExpressions = _queryContext.getSelectExpressions();

    Preconditions.checkState(selectExpressions.size() == 1, "");

    FunctionContext functionContext = selectExpressions.get(0).getFunction();
    String funcName = functionContext.getFunctionName();
    Preconditions.checkState(funcName.equals("jointable"), "funName doesn't equal to jointable:" + funcName);

    FilterPlanNode filterPlanNode = new FilterPlanNode(_indexSegment, _queryContext);
    BaseFilterOperator filterOperator = filterPlanNode.run();

    List<ExpressionContext> arguments = functionContext.getArguments();
    String leftJoinKey = arguments.get(0).getLiteral().toString().replaceAll("'", "");
    String rightTableName = arguments.get(1).getLiteral().toString().replaceAll("'", "");
    String rightJoinKey = arguments.get(2).getLiteral().toString().replaceAll("'", "");
    List<String> leftFilterColumns = new ArrayList<>();
    String leftFilterColumnsStr = arguments.get(3).getLiteral().toString().replaceAll("'", "");
    if (!leftFilterColumnsStr.isEmpty()) {
      leftFilterColumns = Splitter.on(",").splitToList(leftFilterColumnsStr);
    }
    List<String> rightFilterColumns = new ArrayList<>();
    String rightFilterColumnsStr = arguments.get(4).getLiteral().toString().replaceAll("'", "");
    if (!rightFilterColumnsStr.isEmpty()) {
      rightFilterColumns = Splitter.on(",").splitToList(rightFilterColumnsStr);
    }
    String filterExpression = arguments.get(5).getLiteral().toString().replaceAll("'", "");
    List<String> leftProjectColumns = new ArrayList<>();
    String lefProjectStr = arguments.get(6).getLiteral().toString().replaceAll("'", "");
    if (!lefProjectStr.isEmpty()) {
      leftProjectColumns = Splitter.on(",").splitToList(lefProjectStr);
    }
    List<String> rightProjectColumns = new ArrayList<>();
    String rightProjectString = arguments.get(7).getLiteral().toString().replaceAll("'", "");
    if (!rightProjectString.isEmpty()) {
      rightProjectColumns = Splitter.on(",").splitToList(rightProjectString);
    }
    Collection<ExpressionContext> expressionsToTransform = new ArrayList<>();
    HashSet<String> leftColumns = new HashSet<>();
    leftColumns.addAll(leftFilterColumns);
    leftColumns.addAll(leftProjectColumns);
    leftColumns.addAll(Collections.singleton(leftJoinKey));
    for (String col : leftColumns) {
      expressionsToTransform.add(ExpressionContext.forIdentifier(col));
    }

    TransformOperator transformOperator =
        new TransformPlanNode(_indexSegment, _queryContext, expressionsToTransform, DocIdSetPlanNode.MAX_DOC_PER_CALL,
            filterOperator).run();

    Map<String, String> queryOptions = _queryContext.getQueryOptions();
    _queryContext.getOrComputeSharedValue(DataTable.class, rightTableName, (tableName) -> {
      String dataTableStr = QueryOptionsUtils.getInMemoryDataTable(queryOptions, tableName);
      try {
        return DataTableFactory.getDataTable(Base64.getDecoder().decode(dataTableStr));
      } catch (IOException e) {
        LOGGER.error("Got exception while deserializing data table:" + e);
        throw new RuntimeException(e);
      }
    });

    return new ReplicatedJoinOperator(_queryContext, numTotalDocs, transformOperator, rightTableName, leftJoinKey,
        rightJoinKey, leftFilterColumns.toArray(new String[leftFilterColumns.size()]),
        rightFilterColumns.toArray(new String[rightFilterColumns.size()]), filterExpression,
        leftProjectColumns.toArray(new String[leftProjectColumns.size()]),
        rightProjectColumns.toArray(new String[rightProjectColumns.size()]));
  }
}
