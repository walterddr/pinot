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
package org.apache.pinot.query.runtime.operator.dynamic;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;


public class DynamicMailboxReceiveOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "DYNAMIC_MAILBOX_RECEIVER";

  private final MailboxReceiveOperator _mailboxReceiveOperator;
  private final Consumer<TransferableBlock> _receivedDataCallback;
  private final DataSchema _dataSchema;
  private final List<Object[]> _dataContainer;

  public DynamicMailboxReceiveOperator(MailboxReceiveOperator mailboxReceiveOperator, DataSchema dataSchema,
      Consumer<TransferableBlock> receivedDataCallback, long jobId, int stageId, VirtualServerAddress serverAddress) {
    super(jobId, stageId, serverAddress);
    _mailboxReceiveOperator = mailboxReceiveOperator;
    _receivedDataCallback = receivedDataCallback;
    _dataSchema = dataSchema;
    _dataContainer = new ArrayList<>();
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_mailboxReceiveOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    TransferableBlock transferableBlock = _mailboxReceiveOperator.nextBlock();
    if (!TransferableBlockUtils.isNoOpBlock(transferableBlock)) {
      if (TransferableBlockUtils.isEndOfStream(transferableBlock)) {
        if (transferableBlock.isErrorBlock()) {
          _receivedDataCallback.accept(transferableBlock);
        } else {
          _receivedDataCallback.accept(new TransferableBlock(_dataContainer, _dataSchema, BaseDataBlock.Type.ROW));
        }
      } else {
        _dataContainer.addAll(transferableBlock.getContainer());
      }
    }
    return transferableBlock;
  }
}
