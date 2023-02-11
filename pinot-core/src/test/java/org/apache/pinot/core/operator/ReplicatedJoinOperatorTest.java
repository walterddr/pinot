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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ReplicatedJoinOperatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ReplicatedJoinOperatorTest");
  private static final String SEGMENT_NAME = "TestReplicatedJoin";

  private static final int NUM_ROWS = 1000;

  private static final int NUM_METRIC_COLUMNS = 3;

  public static IndexSegment _indexSegment;
  private String[] _columns;
  private QueryContext _queryContext;

  private String _dataTableName;
  private DataTable _dataTable;

  private void setUpDataTable()
      throws IOException {
    DataSchema dataSchema = new DataSchema(new String[]{"a", "b", "c"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.STRING});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int rowId = 0; rowId < NUM_ROWS; rowId++) {
      dataTableBuilder.startRow();
      if (rowId % 2 == 0) {
        dataTableBuilder.setColumn(0, "1");
        dataTableBuilder.setColumn(1, "2");
        dataTableBuilder.setColumn(2, "3");
      } else {
        dataTableBuilder.setColumn(0, "3");
        dataTableBuilder.setColumn(1, "2");
        dataTableBuilder.setColumn(2, "1");
      }
      dataTableBuilder.finishRow();
    }

    _dataTableName = "rightTable";
    _dataTable = dataTableBuilder.build();
  }

  private void setupSegment()
      throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build(),
            buildSchema());
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();

      ImmutableList<String> values = ImmutableList.of("1", "4", "2");
      for (int j = 0; j < _columns.length; j++) {
        String metricName = _columns[j];
        map.put(metricName, values.get(j));
      }
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.heap);
  }

  private Schema buildSchema() {
    Schema schema = new Schema();

    ImmutableList<String> columnNames = ImmutableList.of("d", "e", "f");

    for (int i = 0; i < NUM_METRIC_COLUMNS; i++) {
      String metricName = columnNames.get(i);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.STRING);
      schema.addField(metricFieldSpec);
      _columns[i] = metricName;
    }
    return schema;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _columns = new String[3];
    setupSegment();
    setUpDataTable();

    StringBuilder queryBuilder = new StringBuilder("SELECT * FROM testTable");
    _queryContext = QueryContextConverterUtils.getQueryContext(queryBuilder.toString());
  }

  @Test
  public void testBasicJoin() {
    String leftJoinKey = "d";
    String rightJoinKey = "a";
    String[] filterColumnsLeft = new String[1];
    filterColumnsLeft[0] = "f";
    String[] filterColumnsRight = new String[1];
    filterColumnsRight[0] = "c";

    String[] projectColumnsLeft = new String[1];
    projectColumnsLeft[0] = "e";
    String[] projectColumnsRight = new String[1];
    projectColumnsRight[0] = "b";

    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalDocs);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(_queryContext, projectionOperator, expressions);

    _queryContext.getOrComputeSharedValue(DataTable.class, _dataTableName, (key) -> _dataTable);
    ReplicatedJoinOperator joinOperator =
        new ReplicatedJoinOperator(_queryContext, 1000, transformOperator, _dataTableName, leftJoinKey, rightJoinKey,
            filterColumnsLeft, filterColumnsRight, "c > f", projectColumnsLeft, projectColumnsRight);

    ArrayList<Object[]> rows = new ArrayList<>();
    SelectionResultsBlock block = joinOperator.getNextBlock();
    Collection<Object[]> rowBlock = block.getRows();
    rows.addAll(rowBlock);
    System.out.println(rows.size());
  }

  @Test
  public void testBasicJoinWithAndFilter() {
    String leftJoinKey = "d";
    String rightJoinKey = "a";
    String[] filterColumnsLeft = new String[1];
    filterColumnsLeft[0] = "f";
    String[] filterColumnsRight = new String[1];
    filterColumnsRight[0] = "c";

    String[] projectColumnsLeft = new String[1];
    projectColumnsLeft[0] = "e";
    String[] projectColumnsRight = new String[1];
    projectColumnsRight[0] = "b";

    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalDocs);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(_queryContext, projectionOperator, expressions);

    _queryContext.getOrComputeSharedValue(DataTable.class, _dataTableName, (key) -> _dataTable);
    ReplicatedJoinOperator joinOperator =
        new ReplicatedJoinOperator(_queryContext, 1000, transformOperator, _dataTableName, leftJoinKey, rightJoinKey,
            filterColumnsLeft, filterColumnsRight, "c > f and c > 0", projectColumnsLeft, projectColumnsRight);

    ArrayList<Object[]> rows = new ArrayList<>();
    SelectionResultsBlock block = joinOperator.getNextBlock();
    Collection<Object[]> rowBlock = block.getRows();
    rows.addAll(rowBlock);
    System.out.println(rows.size());
  }

  @Test
  public void testBasicJoinNoMatch() {
    String leftJoinKey = "d";
    String rightJoinKey = "a";
    String[] filterColumnsLeft = new String[1];
    filterColumnsLeft[0] = "f";
    String[] filterColumnsRight = new String[1];
    filterColumnsRight[0] = "c";

    String[] projectColumnsLeft = new String[1];
    projectColumnsLeft[0] = "e";
    String[] projectColumnsRight = new String[1];
    projectColumnsRight[0] = "b";

    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalDocs);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(_queryContext, projectionOperator, expressions);

    _queryContext.getOrComputeSharedValue(DataTable.class, _dataTableName, (key) -> _dataTable);
    ReplicatedJoinOperator joinOperator =
        new ReplicatedJoinOperator(_queryContext, 1000, transformOperator, _dataTableName, leftJoinKey, rightJoinKey,
            filterColumnsLeft, filterColumnsRight, "c == f", projectColumnsLeft, projectColumnsRight);

    ArrayList<Object[]> rows = new ArrayList<>();
    SelectionResultsBlock block = joinOperator.getNextBlock();
    Collection<Object[]> rowBlock = block.getRows();
    rows.addAll(rowBlock);
    System.out.println(rows.size());
  }

  @Test
  public void testBasicJoinOrMatch() {
    String leftJoinKey = "d";
    String rightJoinKey = "a";
    String[] filterColumnsLeft = new String[1];
    filterColumnsLeft[0] = "f";
    String[] filterColumnsRight = new String[1];
    filterColumnsRight[0] = "c";

    String[] projectColumnsLeft = new String[1];
    projectColumnsLeft[0] = "e";
    String[] projectColumnsRight = new String[1];
    projectColumnsRight[0] = "b";

    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalDocs);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(_queryContext, projectionOperator, expressions);

    _queryContext.getOrComputeSharedValue(DataTable.class, _dataTableName, (key) -> _dataTable);
    ReplicatedJoinOperator joinOperator =
        new ReplicatedJoinOperator(_queryContext, 1000, transformOperator, _dataTableName, leftJoinKey, rightJoinKey,
            filterColumnsLeft, filterColumnsRight, "c == f or c > 0", projectColumnsLeft, projectColumnsRight);

    ArrayList<Object[]> rows = new ArrayList<>();
    SelectionResultsBlock block = joinOperator.getNextBlock();
    Collection<Object[]> rowBlock = block.getRows();
    rows.addAll(rowBlock);
    System.out.println(rows.size());
  }
}
