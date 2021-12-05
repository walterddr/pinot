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
package org.apache.pinot.core.query.reduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.UnboundedConcurrentIndexedTable;
import org.apache.pinot.core.operator.combine.GroupByOrderByCombineOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.core.util.QueryOptionsUtils;
import org.apache.pinot.core.util.trace.TraceRunnable;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingGroupByDataTableReducer implements StreamingReducer {
  private static final int MIN_DATA_TABLES_FOR_CONCURRENT_REDUCE = 2; // TBD, find a better value.

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final int _numAggregationFunctions;
  private final List<ExpressionContext> _groupByExpressions;
  private final int _numGroupByExpressions;
  private final int _numColumns;
  private final boolean _preserveType;
  private final boolean _sqlQuery;

  private DataSchema _dataSchema;
  private DataTableReducerContext _reducerContext;
  private IndexedTable _indexedTable;

  StreamingGroupByDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    _groupByExpressions = queryContext.getGroupByExpressions();
    assert _groupByExpressions != null;
    _numGroupByExpressions = _groupByExpressions.size();
    _numColumns = _numAggregationFunctions + _numGroupByExpressions;
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    _preserveType = QueryOptionsUtils.isPreserveType(queryOptions);
    _sqlQuery = queryContext.getBrokerRequest().getPinotQuery() != null;
  }

  @Override
  public void init(DataTableReducerContext dataTableReducerContext) {
    _reducerContext = dataTableReducerContext;
  }

  /**
   * Reduces and sets group by results into ResultTable, if responseFormat = sql
   * By default, sets group by results into GroupByResults
   */
  @Override
  public void reduce(ServerRoutingInstance key, DataTable dataTable) {
    _dataSchema = _dataSchema == null ? dataTable.getDataSchema() : _dataSchema;
    try {
      appendIndexedTable(_dataSchema, dataTable, _reducerContext);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public BrokerResponseNative seal() {
    try {
      BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
      Iterator<Record> sortedIterator = _indexedTable.iterator();
      DataSchema prePostAggregationDataSchema = getPrePostAggregationDataSchema(_dataSchema);
      ColumnDataType[] columnDataTypes = prePostAggregationDataSchema.getColumnDataTypes();
      int numColumns = columnDataTypes.length;
      int limit = _queryContext.getLimit();
      List<Object[]> rows = new ArrayList<>(limit);

      // SQL query with SQL group-by mode and response format

      PostAggregationHandler postAggregationHandler =
          new PostAggregationHandler(_queryContext, prePostAggregationDataSchema);
      FilterContext havingFilter = _queryContext.getHavingFilter();
      if (havingFilter != null) {
        HavingFilterHandler havingFilterHandler = new HavingFilterHandler(havingFilter, postAggregationHandler);
        while (rows.size() < limit && sortedIterator.hasNext()) {
          Object[] row = sortedIterator.next().getValues();
          extractFinalAggregationResults(row);
          for (int i = 0; i < numColumns; i++) {
            row[i] = columnDataTypes[i].convert(row[i]);
          }
          if (havingFilterHandler.isMatch(row)) {
            rows.add(row);
          }
        }
      } else {
        for (int i = 0; i < limit && sortedIterator.hasNext(); i++) {
          Object[] row = sortedIterator.next().getValues();
          extractFinalAggregationResults(row);
          for (int j = 0; j < numColumns; j++) {
            row[j] = columnDataTypes[j].convert(row[j]);
          }
          rows.add(row);
        }
      }
      DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();
      ColumnDataType[] resultColumnDataTypes = resultDataSchema.getColumnDataTypes();
      int numResultColumns = resultColumnDataTypes.length;
      int numResultRows = rows.size();
      List<Object[]> resultRows = new ArrayList<>(numResultRows);
      for (Object[] row : rows) {
        Object[] resultRow = postAggregationHandler.getResult(row);
        for (int i = 0; i < numResultColumns; i++) {
          resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
        }
        resultRows.add(resultRow);
      }
      brokerResponseNative.setResultTable(new ResultTable(resultDataSchema, resultRows));
      return brokerResponseNative;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to extract the final aggregation results for the given row (in-place).
   */
  private void extractFinalAggregationResults(Object[] row) {
    for (int i = 0; i < _numAggregationFunctions; i++) {
      int valueIndex = i + _numGroupByExpressions;
      row[valueIndex] = _aggregationFunctions[i].extractFinalResult(row[valueIndex]);
    }
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  private DataSchema getPrePostAggregationDataSchema(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = new ColumnDataType[_numColumns];
    System.arraycopy(dataSchema.getColumnDataTypes(), 0, columnDataTypes, 0, _numGroupByExpressions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      columnDataTypes[i + _numGroupByExpressions] = _aggregationFunctions[i].getFinalResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  private void appendIndexedTable(DataSchema dataSchema, DataTable dataTable, DataTableReducerContext reducerContext)
      throws TimeoutException {
    int limit = _queryContext.getLimit();
    // TODO: Make minTrimSize configurable
    int trimSize = GroupByUtils.getTableCapacity(limit);
    // NOTE: For query with HAVING clause, use trimSize as resultSize to ensure the result accuracy.
    // TODO: Resolve the HAVING clause within the IndexedTable before returning the result
    int resultSize = _queryContext.getHavingFilter() != null ? trimSize : limit;
    int trimThreshold = reducerContext.getGroupByTrimThreshold();

    // TODO: make it threadsafe.
    _indexedTable = _indexedTable != null ? _indexedTable
        : new ConcurrentIndexedTable(dataSchema, _queryContext, resultSize, trimSize, trimThreshold);
    int numRows = dataTable.getNumberOfRows();
    ColumnDataType[] storedColumnDataTypes = dataSchema.getStoredColumnDataTypes();

    for (int rowId = 0; rowId < numRows; rowId++) {
      Object[] values = new Object[_numColumns];
      for (int colId = 0; colId < _numColumns; colId++) {
        switch (storedColumnDataTypes[colId]) {
          case INT:
            values[colId] = dataTable.getInt(rowId, colId);
            break;
          case LONG:
            values[colId] = dataTable.getLong(rowId, colId);
            break;
          case FLOAT:
            values[colId] = dataTable.getFloat(rowId, colId);
            break;
          case DOUBLE:
            values[colId] = dataTable.getDouble(rowId, colId);
            break;
          case STRING:
            values[colId] = dataTable.getString(rowId, colId);
            break;
          case BYTES:
            values[colId] = dataTable.getBytes(rowId, colId);
            break;
          case OBJECT:
            values[colId] = dataTable.getObject(rowId, colId);
            break;
          // Add other aggregation intermediate result / group-by column type supports here
          default:
            throw new IllegalStateException();
        }
      }
      _indexedTable.upsert(new Record(values));
    }
  }
}
