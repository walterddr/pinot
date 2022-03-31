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
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This {@code MailboxSendOperator} is created to send {@link DataTableBlock}s to the receiving end.
 */
public class MailboxSendOperator extends BaseOperator<DataTableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendOperator.class);
  private static final Set<RelDistribution.Type> SUPPORTED_EXCHANGE_TYPE =
      ImmutableSet.of(RelDistribution.Type.SINGLETON, RelDistribution.Type.RANDOM_DISTRIBUTED,
          RelDistribution.Type.BROADCAST_DISTRIBUTED);

  private final List<ServerInstance> _receivingStageInstances;
  private final RelDistribution.Type _exchangeType;
  private final String _serverHostName;
  private final int _serverPort;
  private final long _jobId;
  private final int _stageId;
  private final MailboxService<Mailbox.MailboxContent> _mailboxService;
  private BaseOperator<DataTableBlock> _dataTableBlockBaseOperator;
  private DataTable _dataTable;

  public MailboxSendOperator(MailboxService<Mailbox.MailboxContent> mailboxService,
      BaseOperator<DataTableBlock> dataTableBlockBaseOperator, List<ServerInstance> receivingStageInstances,
      RelDistribution.Type exchangeType, String hostName, int port, long jobId, int stageId) {
    _mailboxService = mailboxService;
    _dataTableBlockBaseOperator = dataTableBlockBaseOperator;
    _receivingStageInstances = receivingStageInstances;
    _exchangeType = exchangeType;
    _serverHostName = hostName;
    _serverPort = port;
    _jobId = jobId;
    _stageId = stageId;
    Preconditions.checkState(SUPPORTED_EXCHANGE_TYPE.contains(_exchangeType),
        String.format("Exchange type '%s' is not supported yet", _exchangeType));
  }

  /**
   * This is a temporary interface for connecting with server API. remove/merge with InstanceResponseOperator once
   * we create a {@link org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl} that can handle the
   * creation of MailboxSendOperator we should not use this API.
   */
  public MailboxSendOperator(MailboxService<Mailbox.MailboxContent> mailboxService, DataTable dataTable,
      List<ServerInstance> receivingStageInstances, RelDistribution.Type exchangeType, String hostName, int port,
      long jobId, int stageId) {
    _mailboxService = mailboxService;
    _dataTable = dataTable;
    _receivingStageInstances = receivingStageInstances;
    _exchangeType = exchangeType;
    _serverHostName = hostName;
    _serverPort = port;
    _jobId = jobId;
    _stageId = stageId;
  }

  @Override
  public String getOperatorName() {
    return null;
  }

  @Override
  public List<Operator> getChildOperators() {
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  protected DataTableBlock getNextBlock() {
    DataTable dataTable;
    DataTableBlock dataTableBlock = null;
    if (_dataTableBlockBaseOperator != null) {
      dataTableBlock = _dataTableBlockBaseOperator.nextBlock();
      dataTable = dataTableBlock.getDataTable();
    } else {
      dataTable = _dataTable;
    }
    boolean isEndOfStream = dataTableBlock == null || DataTableBlockUtils.isEndOfStream(dataTableBlock);
    try {
      switch (_exchangeType) {
        // TODO: random and singleton distribution should've been selected in planning phase.
        case SINGLETON:
        case RANDOM_DISTRIBUTED:
          // TODO: make random distributed actually random, this impl only sends data to the first instances.
          for (ServerInstance serverInstance : _receivingStageInstances) {
            sendDataTableBlock(serverInstance, dataTable, isEndOfStream);
            // we no longer need to send data to the rest of the receiving instances, but we still need to transfer
            // the dataTable over indicating that we are a potential sender. thus next time a random server is selected
            // it might still be useful.
            dataTable = DataTableBlockUtils.getEmptyDataTable(dataTable.getDataSchema());
          }
          break;
        case BROADCAST_DISTRIBUTED:
          for (ServerInstance serverInstance : _receivingStageInstances) {
            sendDataTableBlock(serverInstance, dataTable, isEndOfStream);
          }
          break;
        case HASH_DISTRIBUTED:
        case RANGE_DISTRIBUTED:
        case ROUND_ROBIN_DISTRIBUTED:
        case ANY:
        default:
          throw new UnsupportedOperationException("Unsupported mailbox exchange type: " + _exchangeType);
      }
    } catch (Exception e) {
      LOGGER.error("Exception occur while sending data via mailbox", e);
    }
    return dataTableBlock;
  }

  private void sendDataTableBlock(ServerInstance serverInstance, DataTable dataTable, boolean isEndOfStream)
      throws IOException {
    String mailboxId = toMailboxId(serverInstance);
    SendingMailbox<Mailbox.MailboxContent> sendingMailbox = _mailboxService.getSendingMailbox(mailboxId);
    Mailbox.MailboxContent mailboxContent = toMailboxContent(mailboxId, dataTable, isEndOfStream);
    sendingMailbox.send(mailboxContent);
    if (mailboxContent.getMetadataMap().containsKey("finished")) {
      sendingMailbox.complete();
    }
  }

  private Mailbox.MailboxContent toMailboxContent(String mailboxId, DataTable dataTable, boolean isEndOfStream)
      throws IOException {
    Mailbox.MailboxContent.Builder builder = Mailbox.MailboxContent.newBuilder().setMailboxId(mailboxId)
        .setPayload(ByteString.copyFrom(new DataTableBlock(dataTable).toBytes()));
    if (isEndOfStream) {
      builder.putMetadata("finished", "true");
    }
    return builder.build();
  }

  private String toMailboxId(ServerInstance serverInstance) {
    return new StringMailboxIdentifier(String.format("%s_%s", _jobId, _stageId), "NULL", _serverHostName, _serverPort,
        serverInstance.getHostname(), serverInstance.getGrpcPort()).toString();
  }
}
