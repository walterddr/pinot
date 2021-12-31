package org.apache.pinot.query.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.routing.WorkerManager;


/**
 * QueryPlanMaker walks top-down from {@link RelRoot} and construct a forest of trees with {@link StageNode}.
 *
 * This class is non-threadsafe. Do not reuse the stage planner for multiple query plans.
 */
public class StagePlanner {
  private final PlannerContext _PlannerContext;
  private final WorkerManager _workerManager;

  private Map<String, StageNode> _queryStageMap;
  private Map<String, StageMetadata> _stageMetadataMap;

  public StagePlanner(PlannerContext PlannerContext, WorkerManager workerManager) {
    _PlannerContext = PlannerContext;
    _workerManager = workerManager;
  }

  public QueryPlan makePlan(RelNode relRoot) {
    // clear the state
    _queryStageMap = new HashMap<>();
    _stageMetadataMap = new HashMap<>();

    // walk the plan and create stages.
    StageNode globalStageRoot = walkRelPlan(relRoot, getNewStageId());

    // global root needs to send results back to the ROOT, a.k.a. the client response node.
    // the last stage is always a broadcast-gather.
    StageNode globalReceiverNode = new MailboxReceiveNode("ROOT", globalStageRoot.getStageId());
    StageNode globalSenderNode = new MailboxSendNode(globalStageRoot, globalReceiverNode.getStageId(),
        RelDistribution.Type.BROADCAST_DISTRIBUTED);
    _queryStageMap.put(globalSenderNode.getStageId(), globalSenderNode);
    StageMetadata stageMetadata = _stageMetadataMap.get(globalSenderNode.getStageId());
    stageMetadata.attach(globalSenderNode);

    _queryStageMap.put(globalReceiverNode.getStageId(), globalReceiverNode);
    StageMetadata globalReceivingStageMetadata = new StageMetadata();
    globalReceivingStageMetadata.attach(globalReceiverNode);
    _stageMetadataMap.put(globalReceiverNode.getStageId(), globalReceivingStageMetadata);

    // assign workers to each stage.
    for (Map.Entry<String, StageMetadata> e : _stageMetadataMap.entrySet()) {
      _workerManager.assignWorkerToStage(e.getKey(), e.getValue());
    }

    return new QueryPlan(_queryStageMap, _stageMetadataMap);
  }

  // non-threadsafe
  private StageNode walkRelPlan(RelNode node, String currentStageId) {
    if (isExchangeNode(node)) {
      // 1. exchangeNode always have only one input, get its input converted as a new stage root.
      StageNode nextStageRoot = walkRelPlan(node.getInput(0), getNewStageId());
      RelDistribution.Type exchangeType = ((LogicalExchange) node).distribution.getType();

      // 2. make an exchange sender and receiver node pair
      StageNode mailboxReceiver = new MailboxReceiveNode(currentStageId, nextStageRoot.getStageId());
      StageNode mailboxSender = new MailboxSendNode(nextStageRoot, mailboxReceiver.getStageId(), exchangeType);

      // 3. put the sender side as a completed stage.
      _queryStageMap.put(mailboxSender.getStageId(), mailboxSender);

      // 4. return the receiver (this is considered as a "virtual table scan" node for its parent.
      return mailboxReceiver;
    } else {
      StageNode stageNode = StageNodeConverter.toStageNode(node, currentStageId);
      List<RelNode> inputs = node.getInputs();
      for (RelNode input : inputs) {
        stageNode.addInput(walkRelPlan(input, currentStageId));
      }
      StageMetadata stageMetadata = _stageMetadataMap.computeIfAbsent(currentStageId, (id) -> new StageMetadata());
      stageMetadata.attach(stageNode);
      return stageNode;
    }
  }

  private boolean isExchangeNode(RelNode node) {
    return (node instanceof LogicalExchange);
  }

  private static String getNewStageId() {
    return UUID.randomUUID().toString();
  }
}
