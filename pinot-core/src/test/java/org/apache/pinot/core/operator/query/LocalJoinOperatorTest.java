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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.RowData;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.LocalJoinPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.SharedValueKey;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class LocalJoinOperatorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "LocalJoinOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_ROWS_LEFT = 1000;
  private static final int NUM_ROWS_RIGHT = 200;
  private static final int NUM_UNIQUE_ROWS = 100;

  private IndexSegment _indexSegment;
  private RowData _rightTable;

  @BeforeClass
  public void setUp()
      throws Exception {
    setUpSegment();
    setUpRightTable();
  }

  /**
   * l1  l2  l3
   *  0   1   2
   *  1   2   3
   *  2   3   4
   * ...
   * 99 100 101
   *  0   1   2
   *  1   2   3
   * ...
   */
  private void setUpSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("l1", DataType.INT)
        .addSingleValueDimension("l2", DataType.LONG).addSingleValueDimension("l3", DataType.STRING).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS_LEFT);
    for (int i = 0; i < NUM_ROWS_LEFT; i++) {
      GenericRow row = new GenericRow();
      int value = i % NUM_UNIQUE_ROWS;
      row.putValue("l1", value);
      row.putValue("l2", value + 1);
      row.putValue("l3", value + 2);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.mmap);
  }

  /**
   * r1  r2  r3
   *  0   2   4
   *  1   3   5
   *  2   4   6
   * ...
   * 99 101 103
   *  0   2   4
   *  1   3   5
   * ...
   */
  private void setUpRightTable() {
    DataSchema dataSchema = new DataSchema(new String[]{"r1", "r2", "r3"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING
    });
    List<Object[]> rows = new ArrayList<>(NUM_ROWS_RIGHT);
    for (int i = 0; i < NUM_ROWS_RIGHT; i++) {
      int value = i % NUM_UNIQUE_ROWS;
      rows.add(new Object[]{value, (long) (value + 2), Integer.toString(value + 4)});
    }
    _rightTable = new RowData(dataSchema, rows);
  }

  private QueryContext getQueryContext(ExpressionContext leftJoinKey, String rightJoinKey, String[] leftFilterColumns,
      String[] rightFilterColumns, String filterPredicate, ExpressionContext[] leftProjectColumns,
      String[] rightProjectColumns) {
    ExpressionContext leftJoinKeysArg = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "leftJoinKeys", Collections.singletonList(leftJoinKey)));
    ExpressionContext rightJoinKeysArg = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "rightJoinKeys",
            Collections.singletonList(ExpressionContext.forIdentifier(rightJoinKey))));
    List<ExpressionContext> leftFilterColumnExpressions = new ArrayList<>(leftFilterColumns.length);
    for (String leftFilterColumn : leftFilterColumns) {
      leftFilterColumnExpressions.add(ExpressionContext.forIdentifier(leftFilterColumn));
    }
    ExpressionContext leftFilterColumnsArg = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "leftFilterColumns", leftFilterColumnExpressions));
    List<ExpressionContext> rightFilterColumnExpressions = new ArrayList<>(rightFilterColumns.length);
    for (String rightFilterColumn : rightFilterColumns) {
      rightFilterColumnExpressions.add(ExpressionContext.forIdentifier(rightFilterColumn));
    }
    ExpressionContext rightFilterColumnsArg = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "rightFilterColumns", rightFilterColumnExpressions));
    ExpressionContext filterPredicateArg = ExpressionContext.forLiteralContext(DataType.STRING, filterPredicate);
    ExpressionContext leftProjectColumnsArg = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "leftProjectColumns", Arrays.asList(leftProjectColumns)));
    List<ExpressionContext> rightProjectColumnExpressions = new ArrayList<>(rightProjectColumns.length);
    for (String rightProjectColumn : rightProjectColumns) {
      rightProjectColumnExpressions.add(ExpressionContext.forIdentifier(rightProjectColumn));
    }
    ExpressionContext rightProjectColumnsArg = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "rightProjectColumns", rightProjectColumnExpressions));
    QueryContext queryContext = new QueryContext.Builder().setSelectExpressions(Collections.singletonList(
            ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "localjoin",
                Arrays.asList(leftJoinKeysArg, rightJoinKeysArg, leftFilterColumnsArg, rightFilterColumnsArg,
                    filterPredicateArg, leftProjectColumnsArg, rightProjectColumnsArg)))))
        .setAliasList(Collections.emptyList()).build();
    queryContext.putSharedValue(RowData.class, SharedValueKey.LOCAL_JOIN_RIGHT_TABLE, _rightTable);
    return queryContext;
  }

  @Test
  public void testSimpleJoin() {
    LocalJoinPlanNode localJoinPlanNode = new LocalJoinPlanNode(_indexSegment,
        getQueryContext(ExpressionContext.forIdentifier("l1"), "r1", new String[0], new String[0], "",
            new ExpressionContext[]{
                ExpressionContext.forIdentifier("l1"), ExpressionContext.forIdentifier(
                "l2"), ExpressionContext.forIdentifier("l3")
            }, new String[]{"r1", "r2", "r3"}));
    SelectionResultsBlock resultsBlock = localJoinPlanNode.run().nextBlock();
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"l1", "l2", "l3", "r1", "r2", "r3"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG,
        ColumnDataType.STRING
    });
    List<Object[]> rows = (List<Object[]>) resultsBlock.getRows();
    assertEquals(rows.size(), 2000);
    for (int i = 0; i < 2000; i++) {
      int keyValue = (i / 2) % NUM_UNIQUE_ROWS;
      Object[] row = rows.get(i);
      assertEquals(row[0], keyValue);
      assertEquals(row[1], (long) (keyValue + 1));
      assertEquals(row[2], Integer.toString(keyValue + 2));
      assertEquals(row[3], keyValue);
      assertEquals(row[4], (long) (keyValue + 2));
      assertEquals(row[5], Integer.toString(keyValue + 4));
    }

    localJoinPlanNode = new LocalJoinPlanNode(_indexSegment,
        getQueryContext(ExpressionContext.forIdentifier("l2"), "r2", new String[0], new String[0], "",
            new ExpressionContext[]{ExpressionContext.forIdentifier("l3"), ExpressionContext.forIdentifier("l1")},
            new String[]{"r3"}));
    resultsBlock = localJoinPlanNode.run().nextBlock();
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"l3", "l1", "r3"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    rows = (List<Object[]>) resultsBlock.getRows();
    assertEquals(rows.size(), 1980);
    for (int i = 0; i < 1980; i++) {
      // 2 - 100
      int keyValue = (i / 2) % 99 + 2;
      Object[] row = rows.get(i);
      assertEquals(row[0], Integer.toString(keyValue + 1));
      assertEquals(row[1], keyValue - 1);
      assertEquals(row[2], Integer.toString(keyValue + 2));
    }

    localJoinPlanNode = new LocalJoinPlanNode(_indexSegment,
        getQueryContext(ExpressionContext.forIdentifier("l3"), "r3", new String[0], new String[0], "",
            new ExpressionContext[]{ExpressionContext.forIdentifier("l2")}, new String[]{"r1", "r3"}));
    resultsBlock = localJoinPlanNode.run().nextBlock();
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"l2", "r1", "r3"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{
        ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.STRING
    });
    rows = (List<Object[]>) resultsBlock.getRows();
    assertEquals(rows.size(), 1960);
    for (int i = 0; i < 1960; i++) {
      // 4 - 101
      int keyValue = (i / 2) % 98 + 4;
      Object[] row = rows.get(i);
      assertEquals(row[0], (long) (keyValue - 1));
      assertEquals(row[1], keyValue - 4);
      assertEquals(row[2], Integer.toString(keyValue));
    }
  }

  @Test
  public void testJoinWithTransform() {
    LocalJoinPlanNode localJoinPlanNode = new LocalJoinPlanNode(_indexSegment,
        getQueryContext(RequestContextUtils.getExpression("cast(l1 + l2 as int)"), "r1", new String[0], new String[0],
            "", new ExpressionContext[]{
                RequestContextUtils.getExpression("cast(l1 + l2 as long)"), RequestContextUtils.getExpression(
                "length(l3)")
            }, new String[]{"r3"}));
    SelectionResultsBlock resultsBlock = localJoinPlanNode.run().nextBlock();
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"cast(plus(l1,l2),'long')", "length(l3)", "r3"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.STRING});
    List<Object[]> rows = (List<Object[]>) resultsBlock.getRows();
    assertEquals(rows.size(), 1000);
    for (int i = 0; i < 1000; i++) {
      // 1, 3, 5, ..., 99
      int keyValue = ((i / 2) % 50) * 2 + 1;
      Object[] row = rows.get(i);
      assertEquals(row[0], (long) keyValue);
      assertEquals(row[1], Integer.toString((keyValue + 3) / 2).length());
      assertEquals(row[2], Integer.toString(keyValue + 4));
    }
  }

  @Test
  public void testJoinWithFilter() {
    LocalJoinPlanNode localJoinPlanNode = new LocalJoinPlanNode(_indexSegment,
        getQueryContext(ExpressionContext.forIdentifier("l1"), "r1", new String[]{"l2"}, new String[]{"r2"},
            "($L.l2 - 10) * 2 < $R.r2", new ExpressionContext[]{
                ExpressionContext.forIdentifier("l1"), ExpressionContext.forIdentifier("l2")
            }, new String[]{"r3"}));
    SelectionResultsBlock resultsBlock = localJoinPlanNode.run().nextBlock();
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"l1", "l2", "r3"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING});
    List<Object[]> rows = (List<Object[]>) resultsBlock.getRows();
    assertEquals(rows.size(), 400);
    for (int i = 0; i < 400; i++) {
      // 0 - 19
      int keyValue = (i / 2) % 20;
      Object[] row = rows.get(i);
      assertEquals(row[0], keyValue);
      assertEquals(row[1], (long) (keyValue + 1));
      assertEquals(row[2], Integer.toString(keyValue + 4));
    }

    localJoinPlanNode = new LocalJoinPlanNode(_indexSegment,
        getQueryContext(ExpressionContext.forIdentifier("l1"), "r1", new String[]{"l1", "l2"}, new String[]{"r1", "r2"},
            "$L.l1 * $R.r1 > $L.l2 + $R.r2 && ($L.l2 - 10) * 2 < $R.r2", new ExpressionContext[]{
                ExpressionContext.forIdentifier("l1"), ExpressionContext.forIdentifier("l2")
            }, new String[]{"r3"}));
    resultsBlock = localJoinPlanNode.run().nextBlock();
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"l1", "l2", "r3"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.STRING});
    rows = (List<Object[]>) resultsBlock.getRows();
    assertEquals(rows.size(), 320);
    for (int i = 0; i < 320; i++) {
      // 4 - 19
      int keyValue = (i / 2) % 16 + 4;
      Object[] row = rows.get(i);
      assertEquals(row[0], keyValue);
      assertEquals(row[1], (long) (keyValue + 1));
      assertEquals(row[2], Integer.toString(keyValue + 4));
    }
  }
}
