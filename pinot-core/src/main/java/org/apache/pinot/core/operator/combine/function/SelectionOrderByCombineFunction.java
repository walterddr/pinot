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
package org.apache.pinot.core.operator.combine.function;

import java.util.Collection;
import java.util.PriorityQueue;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SelectionOrderByCombineFunction implements CombineFunction<SelectionResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionOrderByCombineFunction.class);
  private final int _numRowsToKeep;

  public SelectionOrderByCombineFunction(QueryContext queryContext) {
    _numRowsToKeep = queryContext.getLimit() + queryContext.getOffset();
  }

  @Override
  public void mergeResultsBlocks(SelectionResultsBlock mergedBlock, SelectionResultsBlock blockToMerge) {
    DataSchema mergedDataSchema = mergedBlock.getDataSchema();
    DataSchema dataSchemaToMerge = blockToMerge.getDataSchema();
    assert mergedDataSchema != null && dataSchemaToMerge != null;
    if (!mergedDataSchema.equals(dataSchemaToMerge)) {
      String errorMessage =
          String.format("Data schema mismatch between merged block: %s and block to merge: %s, drop block to merge",
              mergedDataSchema, dataSchemaToMerge);
      // NOTE: This is segment level log, so log at debug level to prevent flooding the log.
      LOGGER.debug(errorMessage);
      mergedBlock.addToProcessingExceptions(
          QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, errorMessage));
      return;
    }

    PriorityQueue<Object[]> mergedRows = mergedBlock.getRowsAsPriorityQueue();
    Collection<Object[]> rowsToMerge = blockToMerge.getRows();
    assert mergedRows != null && rowsToMerge != null;
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge, _numRowsToKeep);
  }

  @Override
  public SelectionResultsBlock convertToMergeableBlock(SelectionResultsBlock resultsBlock) {
    // This may create a copy or return the same instance. Anyway, this operator is the owner of the
    // value now, so it can mutate it.
    return resultsBlock.convertToPriorityQueueBased();
  }
}
