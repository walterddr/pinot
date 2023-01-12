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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.combine.merger.ResultBlockMerger;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of the combine operator.
 * <p>Combine operator uses multiple worker threads to process segments in parallel, and uses the main thread to merge
 * the results blocks from the processed segments. It can early-terminate the query to save the system resources if it
 * detects that the merged results can already satisfy the query, or the query is already errored out or timed out.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseSingleBlockCombineOperator<T extends BaseResultsBlock>
    extends BaseCombineOperator<BaseResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSingleBlockCombineOperator.class);

  protected final ResultBlockMerger<T> _combineFunction;
  // Use an AtomicInteger to track the next operator to execute
  protected final AtomicInteger _nextOperatorId = new AtomicInteger();
  // Use a BlockingQueue to store the intermediate results blocks
  protected final BlockingQueue<BaseResultsBlock> _blockingQueue = new LinkedBlockingQueue<>();
  protected final AtomicLong _totalWorkerThreadCpuTimeNs = new AtomicLong(0);

  protected BaseSingleBlockCombineOperator(ResultBlockMerger<T> combineFunction, List<Operator> operators,
      QueryContext queryContext, ExecutorService executorService) {
    super(operators, queryContext, executorService);
    _combineFunction = combineFunction;
  }

  @Override
  protected BaseResultsBlock getNextBlock() {
    startProcess();
    BaseResultsBlock mergedBlock;
    try {
      mergedBlock = mergeResults();
    } catch (InterruptedException | EarlyTerminationException e) {
      Exception killedErrorMsg = Tracing.getThreadAccountant().getErrorStatus();
      throw new QueryCancelledException(
          "Cancelled while merging results blocks"
              + (killedErrorMsg == null ? StringUtils.EMPTY : " " + killedErrorMsg), e);
    } catch (Exception e) {
      LOGGER.error("Caught exception while merging results blocks (query: {})", _queryContext, e);
      mergedBlock = new ExceptionResultsBlock(QueryException.getException(QueryException.INTERNAL_ERROR, e));
    } finally {
      stopProcess();
    }
    /*
     * _numTasks are number of async tasks submitted to the _executorService, but it does not mean Pinot server
     * use those number of threads to concurrently process segments. Instead, if _executorService thread pool has
     * less number of threads than _numTasks, the number of threads that used to concurrently process segments equals
     * to the pool size.
     * TODO: Get the actual number of query worker threads instead of using the default value.
     */
    int numServerThreads = Math.min(_numTasks, ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
    CombineOperatorUtils.setExecutionStatistics(mergedBlock, _operators, _totalWorkerThreadCpuTimeNs.get(),
        numServerThreads);
    return mergedBlock;
  }

  /**
   * Executes query on one or more segments in a worker thread.
   */
  @Override
  protected void processSegments() {
    int operatorId;
    while ((operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      T resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        resultsBlock = (T) operator.nextBlock();
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }

      if (_combineFunction.isQuerySatisfied(resultsBlock)) {
        // Query is satisfied, skip processing the remaining segments
        _blockingQueue.offer(resultsBlock);
        return;
      } else {
        _blockingQueue.offer(resultsBlock);
      }
    }
  }

  /**
   * Invoked when {@link #processSegments()} throws exception/error.
   */
  protected void onProcessSegmentsException(Throwable t) {
    _blockingQueue.offer(new ExceptionResultsBlock(t));
  }

  /**
   * Invoked when {@link #processSegments()} is finished (called in the finally block).
   */
  protected void onProcessSegmentsFinish() {
  }

  /**
   * Merges the results from the worker threads into a results block.
   */
  @Override
  protected BaseResultsBlock mergeResults()
      throws Exception {
    T mergedBlock = null;
    int numBlocksMerged = 0;
    long endTimeMs = _queryContext.getEndTimeMs();
    while (numBlocksMerged < _numOperators) {
      // Timeout has reached, shouldn't continue to process. `_blockingQueue.poll` will continue to return blocks even
      // if negative timeout is provided; therefore an extra check is needed
      long waitTimeMs = endTimeMs - System.currentTimeMillis();
      if (waitTimeMs <= 0) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      BaseResultsBlock blockToMerge = _blockingQueue.poll(waitTimeMs, TimeUnit.MILLISECONDS);
      if (blockToMerge == null) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      if (blockToMerge.getProcessingExceptions() != null) {
        // Caught exception while processing segment, skip merging the remaining results blocks and directly return the
        // exception
        return blockToMerge;
      }
      if (mergedBlock == null) {
        mergedBlock = _combineFunction.convertToMergeableBlock((T) blockToMerge);
      } else {
        _combineFunction.mergeResultsBlocks(mergedBlock, (T) blockToMerge);
      }
      numBlocksMerged++;
      if (_combineFunction.isQuerySatisfied(mergedBlock)) {
        // Query is satisfied, skip merging the remaining results blocks
        return mergedBlock;
      }
    }
    return mergedBlock;
  }
}
