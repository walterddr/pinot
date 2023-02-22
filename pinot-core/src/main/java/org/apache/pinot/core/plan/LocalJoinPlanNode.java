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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.data.RowData;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.LocalJoinOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SharedValueKey;


/**
 * The <code>LocalJoinPlanNode</code> class provides the execution plan for local join query with arguments:
 * - leftJoinKeys: list of expressions (currently must be 1 expression)
 * - rightJoinKeys: list of identifiers (currently must be 1 identifier)
 * - leftFilterColumns: list of identifiers
 * - rightFilterColumns: list of identifiers
 * - filterPredicate: literal, where identifier has '$L.' or '$R.' prefix
 * - leftProjectColumns: list of expressions
 * - rightProjectColumns: list of identifiers
 */
public class LocalJoinPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;

  public LocalJoinPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
  }

  @Override
  public Operator<SelectionResultsBlock> run() {
    Preconditions.checkState(_queryContext.getSharedValue(RowData.class, SharedValueKey.LOCAL_JOIN_RIGHT_TABLE) != null,
        "Right table must be set");
    List<ExpressionContext> arguments = _queryContext.getSelectExpressions().get(0).getFunction().getArguments();
    Preconditions.checkArgument(arguments.size() == 7, "Expecting 7 arguments, got: %s", arguments.size());
    ExpressionContext leftJoinKey = getLeftJoinKey(arguments.get(0));
    String rightJoinKey = getRightJoinKey(arguments.get(1));
    String[] leftFilterColumns = getLeftFilterColumns(arguments.get(2));
    String[] rightFilterColumns = getRightFilterColumns(arguments.get(3));
    String filterPredicate = getFilterPredicate(arguments.get(4));
    ExpressionContext[] leftProjectColumns = getLeftProjectColumns(arguments.get(5));
    String[] rightProjectColumns = getRightProjectColumns(arguments.get(6));

    Set<ExpressionContext> transformExpressions = new HashSet<>();
    transformExpressions.add(leftJoinKey);
    for (String leftFilterColumn : leftFilterColumns) {
      transformExpressions.add(ExpressionContext.forIdentifier(leftFilterColumn));
    }
    transformExpressions.addAll(Arrays.asList(leftProjectColumns));
    TransformOperator transformOperator = new TransformPlanNode(_indexSegment, _queryContext, transformExpressions,
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();

    return new LocalJoinOperator(_indexSegment, _queryContext, transformOperator, leftJoinKey, rightJoinKey,
        leftFilterColumns, rightFilterColumns, filterPredicate, leftProjectColumns, rightProjectColumns);
  }

  private static ExpressionContext getLeftJoinKey(ExpressionContext expression) {
    FunctionContext function = expression.getFunction();
    Preconditions.checkArgument(function != null && function.getArguments().size() == 1, "Illegal leftJoinKeys: %s",
        expression);
    return function.getArguments().get(0);
  }

  private static String getRightJoinKey(ExpressionContext expression) {
    FunctionContext function = expression.getFunction();
    Preconditions.checkArgument(function != null && function.getArguments().size() == 1, "Illegal rightJoinKeys: %s",
        expression);
    String rightJoinKey = function.getArguments().get(0).getIdentifier();
    Preconditions.checkArgument(rightJoinKey != null, "Illegal rightJoinKeys: %s", expression);
    return rightJoinKey;
  }

  private static String[] getLeftFilterColumns(ExpressionContext expression) {
    FunctionContext function = expression.getFunction();
    Preconditions.checkArgument(function != null, "Illegal leftFilterColumns: %s", expression);
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    String[] leftFilterColumns = new String[numArguments];
    for (int i = 0; i < numArguments; i++) {
      ExpressionContext argument = arguments.get(i);
      Preconditions.checkArgument(argument.getIdentifier() != null, "Illegal leftFilterColumns: %s", expression);
      leftFilterColumns[i] = argument.getIdentifier();
    }
    return leftFilterColumns;
  }

  private static String[] getRightFilterColumns(ExpressionContext expression) {
    FunctionContext function = expression.getFunction();
    Preconditions.checkArgument(function != null, "Illegal rightFilterColumns: %s", expression);
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    String[] rightFilterColumns = new String[numArguments];
    for (int i = 0; i < numArguments; i++) {
      ExpressionContext argument = arguments.get(i);
      Preconditions.checkArgument(argument.getIdentifier() != null, "Illegal rightFilterColumns: %s", expression);
      rightFilterColumns[i] = argument.getIdentifier();
    }
    return rightFilterColumns;
  }

  private static String getFilterPredicate(ExpressionContext expression) {
    LiteralContext literal = expression.getLiteral();
    Preconditions.checkArgument(literal != null && literal.getType() == FieldSpec.DataType.STRING,
        "Illegal filterPredicate: %s", expression);
    return (String) literal.getValue();
  }

  private static ExpressionContext[] getLeftProjectColumns(ExpressionContext expression) {
    FunctionContext function = expression.getFunction();
    Preconditions.checkArgument(function != null, "Illegal leftProjectColumns: %s", expression);
    return function.getArguments().toArray(new ExpressionContext[0]);
  }

  private static String[] getRightProjectColumns(ExpressionContext expression) {
    FunctionContext function = expression.getFunction();
    Preconditions.checkArgument(function != null, "Illegal rightProjectColumns: %s", expression);
    List<ExpressionContext> arguments = function.getArguments();
    int numArguments = arguments.size();
    String[] rightProjectColumns = new String[numArguments];
    for (int i = 0; i < numArguments; i++) {
      ExpressionContext argument = arguments.get(i);
      Preconditions.checkArgument(argument.getIdentifier() != null, "Illegal rightProjectColumns: %s", expression);
      rightProjectColumns[i] = argument.getIdentifier();
    }
    return rightProjectColumns;
  }
}
