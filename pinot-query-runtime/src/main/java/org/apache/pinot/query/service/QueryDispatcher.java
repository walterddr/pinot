package org.apache.pinot.query.service;

import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedQueryPlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);

  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();

  public QueryDispatcher() {
  }

  public List<DataTable> submitAndReduce(long requestId, QueryPlan queryPlan,
      MailboxService<Mailbox.MailboxContent> mailboxService, long timeoutNano)
      throws Exception {
    // submit all the distributed stages.
    String reduceStageId = submit(requestId, queryPlan);

    // run reduce stage.
    MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(reduceStageId);
    MailboxReceiveOperator mailboxReceiveOperator = createReduceStageOperator(mailboxService,
        queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(), String.valueOf(requestId),
        reduceNode.getSenderStageId(), mailboxService.getHostname(), mailboxService.getMailboxPort());

    List<DataTable> queryResults = new ArrayList<>();
    long timeoutWatermark = System.nanoTime() + timeoutNano;
    while (System.nanoTime() < timeoutWatermark) {
      DataTableBlock dataTableBlock = mailboxReceiveOperator.nextBlock();
      queryResults.add(dataTableBlock.getDataTable());
      if (DataTableBlockUtils.isEndOfStream(dataTableBlock)) {
        break;
      }
    }
    return queryResults;
  }

  public String submit(long requestId, QueryPlan queryPlan) throws Exception {
    String reduceStageId = null;
    for (Map.Entry<String, StageMetadata> stage : queryPlan.getStageMetadataMap().entrySet()) {
      String stageId = stage.getKey();
      // stage rooting at a mailbox receive node means reduce stage.
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        reduceStageId = stageId;
      } else {
        List<ServerInstance> serverInstances = stage.getValue().getServerInstances();
        for (ServerInstance serverInstance : serverInstances) {
          String host = serverInstance.getHostname();
          int port = serverInstance.getPort();
          DispatchClient client = getOrCreateDispatchClient(host, port);
          Worker.QueryResponse response = client.submit(Worker.QueryRequest.newBuilder().setQueryPlan(
                  QueryPlanSerDeUtils.serialize(constructDistributedQueryPlan(queryPlan, stageId, serverInstance)))
              .putMetadata("REQUEST_ID", String.valueOf(requestId))
              .putMetadata("SERVER_INSTANCE_HOST", serverInstance.getHostname())
              .putMetadata("SERVER_INSTANCE_PORT", String.valueOf(serverInstance.getGrpcPort())).build());
          if (response.containsMetadata("ERROR")) {
            throw new RuntimeException(
                String.format("Unable to execute query plan at stage %s on server %s: ERROR: %s", stageId,
                    serverInstance, response));
          }
        }
      }
    }
    return reduceStageId;
  }

  protected MailboxReceiveOperator createReduceStageOperator(MailboxService<Mailbox.MailboxContent> mailboxService,
      List<ServerInstance> sendingInstances, String jobId, String stageId, String hostname, int port) {
    MailboxReceiveOperator mailboxReceiveOperator = new MailboxReceiveOperator(mailboxService,
        RelDistribution.Type.ANY, sendingInstances, hostname, port, jobId, stageId);
    return mailboxReceiveOperator;
  }

  public static DistributedQueryPlan constructDistributedQueryPlan(QueryPlan queryPlan, String stageId,
      ServerInstance serverInstance) {
    return new DistributedQueryPlan(stageId, serverInstance, queryPlan.getQueryStageMap().get(stageId),
            queryPlan.getStageMetadataMap());
  }

  private DispatchClient getOrCreateDispatchClient(String host, int port) {
    String key = String.format("%s_%d", host, port);
    return _dispatchClientMap.computeIfAbsent(key, k -> new DispatchClient(host, port));
  }

  public static class DispatchClient {
    private final PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub _blockingStub;
    public DispatchClient(String host, int port) {
      ManagedChannelBuilder managedChannelBuilder = ManagedChannelBuilder
          .forAddress(host, port)
          .usePlaintext();
      _blockingStub = PinotQueryWorkerGrpc.newBlockingStub(managedChannelBuilder.build());
    }
    public Worker.QueryResponse submit(Worker.QueryRequest request) {
      return _blockingStub.submit(request);
    }
  }
}
