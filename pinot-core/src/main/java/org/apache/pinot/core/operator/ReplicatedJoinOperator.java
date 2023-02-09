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
package org.apache.pinot.core.operator;

import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * The <code>ReplicatedJoinOperator</code> class provides the operator for join an on disk table with an inmemory
 * table and optionally apply filter post join
 *
 * JOIN_TABLE(
 */
@SuppressWarnings("rawtypes")
public class ReplicatedJoinOperator extends BaseOperator<SelectionResultsBlock> {

  private static final String EXPLAIN_NAME = "AGGREGATE";
  private final TransformOperator _leftTableStream;
  private final DataTable _rightTableMaterialized;
  private final String[] _filterColumnsLeft;
  private final String[] _filterColumnsRight;
  private final JexlEngine _jexl;
  private final JexlExpression _jexlExpression;
  private String[] _projectColumnsLeft;

  private HashSet<String> _fetchColumnsLeft;
  private final String[] _projectColumnsRight;
  private int _numDocsScanned = 0;

  private final int _numDocs;
  boolean _inited = false;
  String _leftJoinKey;
  String _rightJoinKey;
  Map<Object, List<Integer>> _keyToDocIdMapping;
  Map<String, Integer> _columnName2IndexMap;
  private DataSchema _resultSchema;

  public ReplicatedJoinOperator(int numDocs, TransformOperator transformOperator, DataTable dataTable,
      String leftJoinKey, String rightJoinKey, String[] filterColumnsLeft, String[] filterColumnsRight,
      String postJoinFilterPredicate, String[] projectColumnsLeft, String[] projectColumnsRight) {
    _numDocs = numDocs;
    _leftTableStream = transformOperator;
    _leftJoinKey = leftJoinKey;
    _rightJoinKey = rightJoinKey;
    _filterColumnsLeft = filterColumnsLeft;
    _filterColumnsRight = filterColumnsRight;
    _projectColumnsLeft = projectColumnsLeft;
    _projectColumnsRight = projectColumnsRight;
    _rightTableMaterialized = dataTable;
    _columnName2IndexMap = new HashMap<>();
    String[] columnNames = _rightTableMaterialized.getDataSchema().getColumnNames();
    for (int i = 0; i < columnNames.length; i++) {
      String colName = columnNames[i];
      _columnName2IndexMap.put(colName, i);
    }
    _fetchColumnsLeft = new HashSet<>();
    _fetchColumnsLeft.addAll(List.of(filterColumnsLeft));
    _fetchColumnsLeft.addAll(List.of(leftJoinKey));
    _fetchColumnsLeft.addAll(List.of(projectColumnsLeft));

    if (StringUtils.isNotEmpty(postJoinFilterPredicate)) {
      _jexl = new JexlBuilder().create();
      _jexlExpression = _jexl.createExpression(postJoinFilterPredicate);
    } else {
      _jexl = null;
      _jexlExpression = null;
    }
    int length = _projectColumnsLeft.length + _projectColumnsRight.length;
    String[] resultColNames = new String[length];
    DataSchema.ColumnDataType[] resultColTypes = new DataSchema.ColumnDataType[length];
    int index = 0;
    for (String col : _projectColumnsLeft) {
      ExpressionContext expression = ExpressionContext.forIdentifier(col);
      TransformResultMetadata expressionMetadata = transformOperator.getResultMetadata(expression);
      resultColTypes[index] = DataSchema.ColumnDataType.fromDataType(expressionMetadata.getDataType(),
          expressionMetadata.isSingleValue());
      resultColNames[index] = expression.toString();
      index = index + 1;
    }
    for (String col : _projectColumnsRight) {
      resultColTypes[index] =
          _rightTableMaterialized.getDataSchema().getColumnDataType(_columnName2IndexMap.get(col));
      resultColNames[index] = col;
      index = index + 1;
    }
    _resultSchema = new DataSchema(resultColNames, resultColTypes);
    Preconditions.checkState(index >= 0, "Missing join key " + _rightJoinKey + " in the input table for join");
  }

  @Override
  protected SelectionResultsBlock getNextBlock() {
    TransformBlock transformBlock = _leftTableStream.nextBlock();
    List<Object[]> rows = new ArrayList<>();
    while (transformBlock != null) {
      //build a map of joinKey -> docid on the rightMaterializedTable
      if (!_inited) {
        _keyToDocIdMapping = new HashMap<>();
        int numRows = _rightTableMaterialized.getNumberOfRows();
        int joinIdx = _columnName2IndexMap.get(_rightJoinKey);
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object val = getDataFromRight(rowId, joinIdx);
          _keyToDocIdMapping.putIfAbsent(val, new ArrayList<>());
          _keyToDocIdMapping.get(val).add(rowId);
        }
        _inited = true;
      }
      //for each block we process on the left table
      //when we stream the left table for each row, join with the records that match in right table..
      //Each row from left can match 0 or more records on the right table
      //output of this phase will generate a list of records  and pull the fields mentioned in
      int numRows = transformBlock.getNumDocs();
      Map<String, Object> blockValSetMap = new HashMap<>();
      for (String key : _fetchColumnsLeft) {
        BlockValSet valSet = transformBlock.getBlockValueSet(key);
        Object valueSet = null;
        switch (valSet.getValueType().getStoredType()) {
          case STRING:
            valueSet = valSet.getStringValuesSV();
            break;
          case BOOLEAN:
          case INT:
            valueSet = valSet.getIntValuesSV();
            break;
          case LONG:
            valueSet = valSet.getLongValuesSV();
            break;
          case FLOAT:
            valueSet = valSet.getFloatValuesSV();
            break;
          case DOUBLE:
            valueSet = valSet.getDoubleValuesSV();
            break;
          case BIG_DECIMAL:
            valueSet = valSet.getBigDecimalValuesSV();
            break;
          default:
            throw new RuntimeException("Unsupported data type:" + valSet.getValueType().getStoredType());
        }
        blockValSetMap.put(key, valueSet);
      }
      Object[] joinColValues = (Object[]) blockValSetMap.get(_leftJoinKey);
      JexlContext jc = new MapContext();
      for (int i = 0; i < numRows; i++) {
        if (_keyToDocIdMapping.containsKey(joinColValues[i])) {
          List<Integer> docIds = _keyToDocIdMapping.get(joinColValues[i]);
          for (int docId : docIds) {
            if (_jexlExpression != null) {
              for (String col : _filterColumnsLeft) {
                Object val = Array.get(blockValSetMap.get(col), i);
                jc.set(col, val);
              }
              for (String col : _filterColumnsRight) {
                Object val = getDataFromRight(docId, _columnName2IndexMap.get(col));
                jc.set(col, val);
              }
              //apply the filters on the joined record.. this can be done within the previous loop as well
              // Create a context and add data
              // Now evaluate the expression, getting the result
              Boolean res = (Boolean) _jexlExpression.evaluate(jc);
              if (!res) {
                continue;
              }
            }
            Object[] joinedRow = new Object[_projectColumnsLeft.length + _projectColumnsRight.length];
            int index = 0;
            for (String col : _projectColumnsLeft) {
              joinedRow[index] = Array.get(blockValSetMap.get(col), i);
              jc.set(col, joinedRow[index]);
              index = index + 1;
            }
            for (String col : _projectColumnsRight) {
              joinedRow[index] = getDataFromRight(docId, _columnName2IndexMap.get(col));
              jc.set(col, joinedRow[index]);
              index = index + 1;
            }
            rows.add(joinedRow);
          }
        }
      }
      transformBlock = _leftTableStream.nextBlock();
    }
    return new SelectionResultsBlock(_resultSchema, rows);
  }

  Object getDataFromRight(int rowId, int colIdx) {
    DataSchema.ColumnDataType dataType =
        _rightTableMaterialized.getDataSchema().getColumnDataType(colIdx);
    switch (dataType) {
      case STRING:
        return _rightTableMaterialized.getString(rowId, colIdx);
      case INT:
      case BOOLEAN:
        return _rightTableMaterialized.getInt(rowId, colIdx);
      case LONG:
        return _rightTableMaterialized.getLong(rowId, colIdx);
      case FLOAT:
        return _rightTableMaterialized.getFloat(rowId, colIdx);
      case DOUBLE:
        return _rightTableMaterialized.getDouble(rowId, colIdx);
      case BIG_DECIMAL:
        return _rightTableMaterialized.getBigDecimal(rowId, colIdx);
      default:
        throw new RuntimeException("Unsupported data type:" + dataType);
    }
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_leftTableStream);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _leftTableStream.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _leftTableStream.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, _numDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(replicatedJoin:");
//    if (_aggregationFunctions.length > 0) {
//      stringBuilder.append(_aggregationFunctions[0].toExplainString());
//      for (int i = 1; i < _aggregationFunctions.length; i++) {
//        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
//      }
//    }
    return stringBuilder.append(')').toString();
  }
}
