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
package org.apache.pinot.core.operator.query;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang.ArrayUtils;
import org.apache.pinot.common.data.RowData;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SharedValueKey;


@SuppressWarnings("rawtypes")
public class LocalJoinOperator extends BaseOperator<SelectionResultsBlock> {
  public static final String EXPLAIN_NAME = "LOCAL_JOIN";
  public static final String LEFT_TABLE_PREFIX = "$L.";
  public static final String RIGHT_TABLE_PREFIX = "$R.";

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final TransformOperator _leftTable;
  private final ExpressionContext _leftJoinKey;
  private final String _rightJoinKey;
  private final String[] _leftFilterColumns;
  private final String[] _rightFilterColumns;
  private final JexlExpression _jexlExpression;
  private final ExpressionContext[] _leftProjectColumns;
  private final String[] _rightProjectColumns;

  private int _numDocsScanned;

  public LocalJoinOperator(IndexSegment indexSegment, QueryContext queryContext, TransformOperator leftTable,
      ExpressionContext leftJoinKey, String rightJoinKey, String[] leftFilterColumns, String[] rightFilterColumns,
      String filterPredicate, ExpressionContext[] leftProjectColumns, String[] rightProjectColumns) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _leftTable = leftTable;
    _leftJoinKey = leftJoinKey;
    _rightJoinKey = rightJoinKey;
    _leftFilterColumns = leftFilterColumns;
    _rightFilterColumns = rightFilterColumns;
    _jexlExpression = filterPredicate.isEmpty() ? null : new JexlBuilder().create().createExpression(filterPredicate);
    _leftProjectColumns = leftProjectColumns;
    _rightProjectColumns = rightProjectColumns;
  }

  @Override
  protected SelectionResultsBlock getNextBlock() {
    RowData rightTable = _queryContext.getSharedValue(RowData.class, SharedValueKey.LOCAL_JOIN_RIGHT_TABLE);

    LookUpContext context = _queryContext.getOrComputeSharedValue(LookUpContext.class, "", k -> {
      // Compute the left table column indexes
      Object2IntMap<ExpressionContext> leftColumnToIndexMap = new Object2IntOpenHashMap<>();
      leftColumnToIndexMap.defaultReturnValue(-1);
      List<ExpressionContext> leftColumns = new ArrayList<>();
      leftColumnToIndexMap.put(_leftJoinKey, 0);
      leftColumns.add(_leftJoinKey);
      String[] leftFilterColumnNames = new String[_leftFilterColumns.length];
      int[] leftFilterColumnIndexes = new int[_leftFilterColumns.length];
      int[] leftProjectColumnIndexes = new int[_leftProjectColumns.length];
      for (int i = 0; i < _leftFilterColumns.length; i++) {
        leftFilterColumnNames[i] = LEFT_TABLE_PREFIX + _leftFilterColumns[i];
        ExpressionContext leftFilterColumn = ExpressionContext.forIdentifier(_leftFilterColumns[i]);
        int index = leftColumnToIndexMap.getInt(leftFilterColumn);
        if (index == -1) {
          index = leftColumnToIndexMap.size();
          leftColumnToIndexMap.put(leftFilterColumn, index);
          leftColumns.add(leftFilterColumn);
        }
        leftFilterColumnIndexes[i] = index;
      }
      for (int i = 0; i < _leftProjectColumns.length; i++) {
        ExpressionContext leftProjectColumn = _leftProjectColumns[i];
        int index = leftColumnToIndexMap.getInt(leftProjectColumn);
        if (index == -1) {
          index = leftColumnToIndexMap.size();
          leftColumnToIndexMap.put(leftProjectColumn, index);
          leftColumns.add(leftProjectColumn);
        }
        leftProjectColumnIndexes[i] = index;
      }

      // Compute the result data schema and right project column indexes
      DataSchema rightDataSchema = rightTable.getDataSchema();
      String[] rightColumnNames = rightDataSchema.getColumnNames();
      ColumnDataType[] rightColumnDataTypes = rightDataSchema.getColumnDataTypes();
      Object2IntMap<String> rightColumnToIndexMap = new Object2IntOpenHashMap<>(rightColumnNames.length);
      rightColumnToIndexMap.defaultReturnValue(-1);
      for (int i = 0; i < rightColumnNames.length; i++) {
        rightColumnToIndexMap.put(rightColumnNames[i], i);
      }
      int numResultColumns = _leftProjectColumns.length + _rightProjectColumns.length;
      String[] resultColumnNames = new String[numResultColumns];
      ColumnDataType[] resultColumnTypes = new ColumnDataType[numResultColumns];
      for (int i = 0; i < _leftProjectColumns.length; i++) {
        ExpressionContext leftProjectColumn = _leftProjectColumns[i];
        resultColumnNames[i] = leftProjectColumn.toString();
        TransformResultMetadata columnMetadata = _leftTable.getResultMetadata(leftProjectColumn);
        resultColumnTypes[i] =
            ColumnDataType.fromDataType(columnMetadata.getDataType(), columnMetadata.isSingleValue());
      }
      int[] rightProjectColumnIndexes = new int[_rightProjectColumns.length];
      for (int i = 0; i < _rightProjectColumns.length; i++) {
        String rightProjectColumn = _rightProjectColumns[i];
        int columnIndex = rightColumnToIndexMap.getInt(rightProjectColumn);
        Preconditions.checkArgument(columnIndex != -1, "Failed to find right project column: %s", rightProjectColumn);
        resultColumnNames[_leftProjectColumns.length + i] = rightColumnNames[columnIndex];
        resultColumnTypes[_leftProjectColumns.length + i] = rightColumnDataTypes[columnIndex];
        rightProjectColumnIndexes[i] = columnIndex;
      }
      DataSchema resultDataSchema = new DataSchema(resultColumnNames, resultColumnTypes);

      // Construct the look-up table and calculate the right filter column indexes
      int rightJoinColumnIndex = rightColumnToIndexMap.getInt(_rightJoinKey);
      Preconditions.checkArgument(rightJoinColumnIndex != -1, "Failed to find right join column: %s", _rightJoinKey);
      Map<Object, List<Object[]>> lookupTable = new HashMap<>();
      List<Object[]> rows = rightTable.getRows();
      for (Object[] row : rows) {
        Object key = row[rightJoinColumnIndex];
        lookupTable.computeIfAbsent(key, k1 -> new ArrayList<>()).add(row);
      }
      String[] rightFilterColumnNames = new String[_rightFilterColumns.length];
      int[] rightFilterColumnIndexes = new int[_rightFilterColumns.length];
      for (int i = 0; i < _rightFilterColumns.length; i++) {
        rightFilterColumnNames[i] = RIGHT_TABLE_PREFIX + _rightFilterColumns[i];
        rightFilterColumnIndexes[i] = rightColumnToIndexMap.getInt(_rightFilterColumns[i]);
        Preconditions.checkArgument(rightFilterColumnIndexes[i] != -1, "Failed to find right filter column: %s",
            _rightFilterColumns[i]);
      }

      LookUpContext lookUpContext = new LookUpContext();
      lookUpContext._resultDataSchema = resultDataSchema;
      lookUpContext._leftColumns = leftColumns.toArray(new ExpressionContext[0]);
      lookUpContext._leftFilterColumnNames = leftFilterColumnNames;
      lookUpContext._leftFilterColumnIndexes = leftFilterColumnIndexes;
      lookUpContext._leftProjectColumnIndexes = leftProjectColumnIndexes;
      lookUpContext._lookupMap = lookupTable;
      lookUpContext._rightFilterColumnNames = rightFilterColumnNames;
      lookUpContext._rightFilterColumnIndexes = rightFilterColumnIndexes;
      lookUpContext._rightProjectColumnIndexes = rightProjectColumnIndexes;
      return lookUpContext;
    });

    List<Object[]> rows = new ArrayList<>();
    Object[][] leftColumnValues = new Object[context._leftColumns.length][];
    TransformBlock transformBlock;
    while ((transformBlock = _leftTable.nextBlock()) != null) {
      int numDocs = transformBlock.getNumDocs();
      _numDocsScanned += numDocs;
      for (int i = 0; i < context._leftColumns.length; i++) {
        ExpressionContext leftColumn = context._leftColumns[i];
        BlockValSet valueSet = transformBlock.getBlockValueSet(leftColumn);
        switch (valueSet.getValueType().getStoredType()) {
          case INT:
            leftColumnValues[i] = ArrayUtils.toObject(valueSet.getIntValuesSV());
            break;
          case LONG:
            leftColumnValues[i] = ArrayUtils.toObject(valueSet.getLongValuesSV());
            break;
          case FLOAT:
            leftColumnValues[i] = ArrayUtils.toObject(valueSet.getFloatValuesSV());
            break;
          case DOUBLE:
            leftColumnValues[i] = ArrayUtils.toObject(valueSet.getDoubleValuesSV());
            break;
          case BIG_DECIMAL:
            leftColumnValues[i] = valueSet.getBigDecimalValuesSV();
            break;
          case STRING:
            leftColumnValues[i] = valueSet.getStringValuesSV();
            break;
          default:
            throw new IllegalStateException("Unsupported value type: " + valueSet.getValueType().getStoredType());
        }
      }
      int numProjectColumns = context._leftProjectColumnIndexes.length + context._rightProjectColumnIndexes.length;
      if (_jexlExpression == null) {
        for (int i = 0; i < numDocs; i++) {
          Object key = leftColumnValues[0][i];
          List<Object[]> rightRows = context._lookupMap.get(key);
          if (rightRows != null) {
            for (Object[] rightRow : rightRows) {
              Object[] row = new Object[numProjectColumns];
              for (int j = 0; j < context._leftProjectColumnIndexes.length; j++) {
                row[j] = leftColumnValues[context._leftProjectColumnIndexes[j]][i];
              }
              for (int j = 0; j < context._rightProjectColumnIndexes.length; j++) {
                row[context._leftProjectColumnIndexes.length + j] = rightRow[context._rightProjectColumnIndexes[j]];
              }
              rows.add(row);
            }
          }
        }
      } else {
        JexlContext jexlContext = new MapContext();
        for (int i = 0; i < numDocs; i++) {
          Object key = leftColumnValues[0][i];
          List<Object[]> rightRows = context._lookupMap.get(key);
          if (rightRows != null) {
            for (int j = 0; j < context._leftFilterColumnNames.length; j++) {
              jexlContext.set(context._leftFilterColumnNames[j],
                  leftColumnValues[context._leftFilterColumnIndexes[j]][i]);
            }
            for (Object[] rightRow : rightRows) {
              for (int j = 0; j < context._rightFilterColumnNames.length; j++) {
                jexlContext.set(context._rightFilterColumnNames[j], rightRow[context._rightFilterColumnIndexes[j]]);
              }
              if ((boolean) _jexlExpression.evaluate(jexlContext)) {
                Object[] row = new Object[numProjectColumns];
                for (int j = 0; j < context._leftProjectColumnIndexes.length; j++) {
                  row[j] = leftColumnValues[context._leftProjectColumnIndexes[j]][i];
                }
                for (int j = 0; j < context._rightProjectColumnIndexes.length; j++) {
                  row[context._leftProjectColumnIndexes.length + j] = rightRow[context._rightProjectColumnIndexes[j]];
                }
                rows.add(row);
              }
            }
          }
        }
      }
    }

    return new SelectionResultsBlock(context._resultDataSchema, rows);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_leftTable);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _leftTable.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = (long) _numDocsScanned * _leftTable.getNumColumnsProjected();
    return new ExecutionStatistics(_numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
        _indexSegment.getSegmentMetadata().getTotalDocs());
  }

  public static class LookUpContext {
    public DataSchema _resultDataSchema;
    public ExpressionContext[] _leftColumns;
    public String[] _leftFilterColumnNames;
    public int[] _leftFilterColumnIndexes;
    public int[] _leftProjectColumnIndexes;
    public Map<Object, List<Object[]>> _lookupMap;
    public String[] _rightFilterColumnNames;
    public int[] _rightFilterColumnIndexes;
    public int[] _rightProjectColumnIndexes;
  }
}
