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
package org.apache.pinot.query.runtime.operator.exchange;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * This class contains the shared logic across all different exchange types for
 * exchanging data across different servers.
 */
public abstract class BlockExchange {
  public static final int DEFAULT_MAX_PENDING_BLOCKS = 1;
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;
  private final List<SendingMailbox> _sendingMailboxes;
  private final BlockSplitter _splitter;
  private final BlockingQueue<TransferableBlock> _queue = new ArrayBlockingQueue<>(DEFAULT_MAX_PENDING_BLOCKS);
  private final OpChainExecutionContext _context;

  public static BlockExchange getExchange(OpChainExecutionContext context, List<SendingMailbox> sendingMailboxes,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> selector, BlockSplitter splitter) {
    switch (exchangeType) {
      case SINGLETON:
        return new SingletonExchange(context, sendingMailboxes, splitter);
      case HASH_DISTRIBUTED:
        return new HashExchange(context, sendingMailboxes, selector, splitter);
      case RANDOM_DISTRIBUTED:
        return new RandomExchange(context, sendingMailboxes, splitter);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchange(context, sendingMailboxes, splitter);
      case ROUND_ROBIN_DISTRIBUTED:
      case RANGE_DISTRIBUTED:
      case ANY:
      default:
        throw new UnsupportedOperationException("Unsupported mailbox exchange type: " + exchangeType);
    }
  }

  protected BlockExchange(OpChainExecutionContext context, List<SendingMailbox> sendingMailboxes,
      BlockSplitter splitter) {
    _context = context;
    _sendingMailboxes = sendingMailboxes;
    _splitter = splitter;
    _context.getMailboxService().submitExchangeRequest(this);
  }

  public boolean send(TransferableBlock block)
      throws Exception {
    _queue.offer(block, _context.getTimeoutMs(), TimeUnit.MILLISECONDS);
    return _queue.remainingCapacity() > 0;
  }

  public TransferableBlock sendBlock() {
    try {
      TransferableBlock block = _queue.poll(_context.getTimeoutMs(), TimeUnit.MILLISECONDS);
      // TODO: make it into a formal callback in constructor
      _context.getMailboxService().getCallback().accept(_context.getId());
      if (block == null) {
        return TransferableBlockUtils.getErrorTransferableBlock(
            new TimeoutException("Timed out while sending data on: " + _context.getId()));
      }
      if (block.isEndOfStreamBlock()) {
        for (SendingMailbox sendingMailbox : _sendingMailboxes) {
          sendBlockToMailbox(sendingMailbox, block);
        }
        return block;
      }
      route(_sendingMailboxes, block);
      return block;
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(
          new TimeoutException("Exception while sending data via mailbox on: " + _context.getId()));
    }
  }

  protected void sendBlockToMailbox(SendingMailbox sendingMailbox, TransferableBlock block)
      throws Exception {
    if (block.isEndOfStreamBlock()) {
      sendingMailbox.send(block);
      sendingMailbox.complete();
      return;
    }

    DataBlock.Type type = block.getType();
    Iterator<TransferableBlock> splits = _splitter.split(block, type, MAX_MAILBOX_CONTENT_SIZE_BYTES);
    while (splits.hasNext()) {
      sendingMailbox.send(splits.next());
    }
  }

  protected abstract void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws Exception;

  // Called when the OpChain gracefully returns.
  // TODO: This is a no-op right now.
  public void close() {
  }

  public void cancel(Throwable t) {
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      sendingMailbox.cancel(t);
    }
  }

  public OpChainId getOpChainId() {
    return _context.getId();
  }
}
