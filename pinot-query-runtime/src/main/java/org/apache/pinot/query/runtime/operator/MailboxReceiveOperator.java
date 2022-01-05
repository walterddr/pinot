package org.apache.pinot.query.runtime.operator;

import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;
import org.apache.pinot.query.runtime.mailbox.MailboxService;
import org.apache.pinot.query.runtime.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.mailbox.StringMailboxIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MailboxReceiveOperator extends BaseOperator<DataTableBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);

  private final MailboxService<Mailbox.MailboxContent> _mailboxService;
  private final RelDistribution.Type _exchangeType;
  private final List<ServerInstance> _sendingStageInstances;
  private final String _hostName;
  private final int _port;
  private final String _jobId;
  private final String _stageId;

  public MailboxReceiveOperator(MailboxService<Mailbox.MailboxContent> mailboxService,
      RelDistribution.Type exchangeType, List<ServerInstance> sendingStageInstances, String hostName, int port,
      String jobId, String stageId) {
    _mailboxService = mailboxService;
    _exchangeType = exchangeType;
    _sendingStageInstances = sendingStageInstances;
    _hostName = hostName;
    _port = port;
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
    // TODO: do a round robin check against all MailboxContentStreamObservers and find which one that has data.
    boolean hasOpenedMailbox = true;
    DataSchema dataSchema = null;
    while (hasOpenedMailbox) {
      hasOpenedMailbox = false;
      for (ServerInstance sendingInstance : _sendingStageInstances) {
        try {
          ReceivingMailbox<Mailbox.MailboxContent> receivingMailbox = _mailboxService.getReceivingMailbox(toMailboxId(sendingInstance));
          // TODO this is not threadsafe.
          // make sure only one thread is checking receiving mailbox and calling receive() then close()
          if (!receivingMailbox.isClosed()) {
            hasOpenedMailbox = true;
            Mailbox.MailboxContent mailboxContent = receivingMailbox.receive();
            if (mailboxContent != null) {
              if (mailboxContent.getMetadataMap().get("FINISHED") != null) {
                receivingMailbox.close();
              }
              ByteBuffer byteBuffer = mailboxContent.getPayload().asReadOnlyByteBuffer();
              if (byteBuffer.hasRemaining()) {
                DataTable dataTable = DataTableFactory.getDataTable(byteBuffer);
                if (dataTable.getNumberOfRows() > 0) {
                  // here we only return data table block when it is not empty.
                  return new DataTableBlock(dataTable);
                }
              }
            }
          }
        } catch (Exception e) {
          LOGGER.error(String.format("Error receiving data from mailbox %s", sendingInstance), e);
        }
      }
    }
    // TODO: we need to at least return one data table with schema if there's no error.
    // we need to condition this on whether there's already things being returned or not.
    return DataTableBlockUtils.getEndOfStreamDataTableBlock();
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }

  private String toMailboxId(ServerInstance serverInstance) {
    return new StringMailboxIdentifier(String.format("%s_%s", _jobId, _stageId), "NULL",
        serverInstance.getHostname(), serverInstance.getGrpcPort(), _hostName, _port).toString();
  }
}
