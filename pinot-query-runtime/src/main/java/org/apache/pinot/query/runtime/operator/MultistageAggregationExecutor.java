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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.IntermediateStageBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Class that executes all aggregation functions (without group-bys) for the multistage AggregateOperator.
 */
public class MultistageAggregationExecutor {
  private final AggType _aggType;
  // The identifier operands for the aggregation function only store the column name. This map contains mapping
  // from column name to their index.
  private final Map<String, Integer> _colNameToIndexMap;
  private final DataSchema _resultSchema;

  private final AggregationFunction[] _aggFunctions;

  // Result holders for each mode.
  private final AggregationResultHolder[] _aggregateResultHolder;
  private final Object[] _mergeResultHolder;

  public MultistageAggregationExecutor(AggregationFunction[] aggFunctions,
      AggType aggType, Map<String, Integer> colNameToIndexMap, DataSchema resultSchema) {
    _aggFunctions = aggFunctions;
    _aggType = aggType;
    _colNameToIndexMap = colNameToIndexMap;
    _resultSchema = resultSchema;

    _aggregateResultHolder = new AggregationResultHolder[aggFunctions.length];
    _mergeResultHolder = new Object[aggFunctions.length];

    for (int i = 0; i < _aggFunctions.length; i++) {
      _aggregateResultHolder[i] = _aggFunctions[i].createAggregationResultHolder();
    }
  }

  /**
   * Performs aggregation for the data in the block.
   */
  public void processBlock(TransferableBlock block, DataSchema inputDataSchema) {
    if (!_aggType.isInputIntermediateFormat()) {
      processAggregate(block, inputDataSchema);
    } else {
      processMerge(block);
    }
  }

  /**
   * @return an empty agg result block for non-group-by aggregation.
   */
  public Object[] constructEmptyAggResultRow() {
    Object[] row = new Object[_aggFunctions.length];
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      row[i] = aggFunction.extractAggregationResult(aggFunction.createAggregationResultHolder());
    }
    return row;
  }

  /**
   * Fetches the result.
   */
  public List<Object[]> getResult() {
    Object[] row = new Object[_aggFunctions.length];
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      Object value;
      switch (_aggType) {
        case LEAF:
          value = aggFunction.extractAggregationResult(_aggregateResultHolder[i]);
          break;
        case INTERMEDIATE:
          value = _mergeResultHolder[i];
          break;
        case FINAL:
          value = aggFunction.extractFinalResult(_mergeResultHolder[i]);
          break;
        case DIRECT:
          Object intermediate = aggFunction.extractAggregationResult(_aggregateResultHolder[i]);
          value = aggFunction.extractFinalResult(intermediate);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported aggTyp: " + _aggType);
      }
      row[i] = value;
    }
    return Collections.singletonList(TypeUtils.canonicalizeRow(row, _resultSchema));
  }

  private void processAggregate(TransferableBlock block, DataSchema inputDataSchema) {
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap =
          getBlockValSetMap(aggregationFunction, block, inputDataSchema);
      aggregationFunction.aggregate(block.getNumRows(), _aggregateResultHolder[i], blockValSetMap);
    }
  }

  private void processMerge(TransferableBlock block) {
    List<Object[]> container = block.getContainer();

    for (int i = 0; i < _aggFunctions.length; i++) {
      for (Object[] row : container) {
        Object intermediateResultToMerge = extractValueFromRow(_aggFunctions[i], row);

        // Not all V1 aggregation functions have null-handling logic. Handle null values before calling merge.
        if (intermediateResultToMerge == null) {
          continue;
        }
        Object mergedIntermediateResult = _mergeResultHolder[i];
        if (mergedIntermediateResult == null) {
          _mergeResultHolder[i] = intermediateResultToMerge;
          continue;
        }

        _mergeResultHolder[i] = _aggFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
      }
    }
  }

  private Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggFunction,
      TransferableBlock block, DataSchema inputDataSchema) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }

    Preconditions.checkState(numExpressions == 1, "Cannot handle more than one identifier in aggregation function.");
    ExpressionContext expression = expressions.get(0);
    Preconditions.checkState(expression.getType().equals(ExpressionContext.Type.IDENTIFIER));
    int index = _colNameToIndexMap.get(expression.getIdentifier());

    DataSchema.ColumnDataType dataType = inputDataSchema.getColumnDataType(index);
    Preconditions.checkState(block.getType().equals(DataBlock.Type.ROW), "Datablock type is not ROW");
    // TODO: If the previous block is not mailbox received, this method is not efficient.  Then getDataBlock() will
    //  convert the unserialized format to serialized format of BaseDataBlock. Then it will convert it back to column
    //  value primitive type.
    return Collections.singletonMap(expression,
        new IntermediateStageBlockValSet(dataType, block.getDataBlock(), index));
  }

  private Object extractValueFromRow(AggregationFunction aggregationFunction, Object[] row) {
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    Preconditions.checkState(expressions.size() == 1, "Only support single expression, got: %s", expressions.size());
    ExpressionContext expr = expressions.get(0);
    ExpressionContext.Type exprType = expr.getType();
    if (exprType.equals(ExpressionContext.Type.IDENTIFIER)) {
      String colName = expr.getIdentifier();
      Object value = row[_colNameToIndexMap.get(colName)];
      return toIntermediateType(value, aggregationFunction);
    }
    Preconditions.checkState(exprType == ExpressionContext.Type.LITERAL, "Unsupported expression type: %s", exprType);
    return expr.getLiteral().getValue();
  }

  // TODO: remove this once planner correctly expects intermediate result type.
  private Object toIntermediateType(Object value, AggregationFunction aggregationFunction) {
    if (aggregationFunction.getType().equals(AggregationFunctionType.BOOLAND)
        || aggregationFunction.getType().equals(AggregationFunctionType.BOOLOR)
        || aggregationFunction.getType().equals(AggregationFunctionType.COUNT)) {
      return (value instanceof Boolean) ? (((Boolean) value) ? 1 : 0)
          : ((value instanceof Number) ? ((Number) value).intValue() : value);
    } else {
      return (value instanceof Boolean) ? (((Boolean) value) ? 1.0 : 0.0)
          : ((value instanceof Number) ? ((Number) value).doubleValue() : value);
    }
  }
}
