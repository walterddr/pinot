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
package org.apache.pinot.query.planner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SetOpNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * A visitor that converts a {@code QueryPlan} into a human-readable string representation.
 *
 * <p>It is currently not used programmatically and cannot be accessed by the user. Instead,
 * it is intended for use in manual debugging (e.g. setting breakpoints and calling QueryPlan#explain()).
 */
public class ExplainPlanStageVisitor implements StageNodeVisitor<StringBuilder, ExplainPlanStageVisitor.Context> {

  private final QueryPlan _queryPlan;

  /**
   * Explains the query plan.
   *
   * @see QueryPlan#explain()
   * @param queryPlan the queryPlan to explain
   * @return a String representation of the query plan tree
   */
  public static String explain(QueryPlan queryPlan) {
    if (queryPlan.getQueryStageMap().isEmpty()) {
      return "EMPTY";
    }

    // the root of a query plan always only has a single node
    QueryServerInstance rootServer = queryPlan.getDispatchablePlanMetadataMap().get(0).getServerInstanceToWorkerIdMap()
        .keySet().iterator().next();
    return explainFrom(queryPlan, queryPlan.getQueryStageMap().get(0), rootServer);
  }

  /**
   * Explains the query plan from a specific point in the subtree, taking {@code rootServer}
   * as the node that is executing this sub-tree. This is helpful for debugging what is happening
   * at a given point in time (for example, printing the tree that will be executed on a
   * local node right before it is executed).
   *
   * @param queryPlan the entire query plan, including non-executed portions
   * @param node the node to begin traversal
   * @param rootServer the server instance that is executing this plan (should execute {@code node})
   *
   * @return a query plan associated with
   */
  public static String explainFrom(QueryPlan queryPlan, StageNode node, QueryServerInstance rootServer) {
    final ExplainPlanStageVisitor visitor = new ExplainPlanStageVisitor(queryPlan);
    return node
        .visit(visitor, new Context(rootServer, 0, "", "", new StringBuilder()))
        .toString();
  }

  private ExplainPlanStageVisitor(QueryPlan queryPlan) {
    _queryPlan = queryPlan;
  }

  private StringBuilder appendInfo(StageNode node, Context context) {
    int stage = node.getStageId();
    context._builder
        .append(context._prefix)
        .append('[')
        .append(stage)
        .append("]@")
        .append(context._host.getHostname())
        .append(':')
        .append(context._host.getPort())
        .append(' ')
        .append(node.explain());
    return context._builder;
  }

  private StringBuilder visitSimpleNode(StageNode node, Context context) {
    appendInfo(node, context).append('\n');
    return node.getInputs().get(0).visit(this, context.next(false, context._host, context._workerId));
  }

  @Override
  public StringBuilder visitAggregate(AggregateNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitWindow(WindowNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitSetOp(SetOpNode setOpNode, Context context) {
    appendInfo(setOpNode, context).append('\n');
    for (StageNode input : setOpNode.getInputs()) {
      input.visit(this, context.next(false, context._host, context._workerId));
    }
    return context._builder;
  }

  @Override
  public StringBuilder visitFilter(FilterNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitJoin(JoinNode node, Context context) {
    appendInfo(node, context).append('\n');
    node.getInputs().get(0).visit(this, context.next(true, context._host, context._workerId));
    node.getInputs().get(1).visit(this, context.next(false, context._host, context._workerId));
    return context._builder;
  }

  @Override
  public StringBuilder visitMailboxReceive(MailboxReceiveNode node, Context context) {
    appendInfo(node, context).append('\n');

    MailboxSendNode sender = (MailboxSendNode) node.getSender();
    int senderStageId = node.getSenderStageId();
    DispatchablePlanMetadata metadata = _queryPlan.getDispatchablePlanMetadataMap().get(senderStageId);
    Map<Integer, Map<String, List<String>>> segments = metadata.getWorkerIdToSegmentsMap();

    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap = metadata.getServerInstanceToWorkerIdMap();
    Iterator<QueryServerInstance> iterator = serverInstanceToWorkerIdMap.keySet().iterator();
    while (iterator.hasNext()) {
      QueryServerInstance queryServerInstance = iterator.next();
      for (int workerId : serverInstanceToWorkerIdMap.get(queryServerInstance)) {
        if (segments.containsKey(workerId)) {
          // always print out leaf stages
          sender.visit(this, context.next(iterator.hasNext(), queryServerInstance, workerId));
        } else {
          if (!iterator.hasNext()) {
            // always print out the last one
            sender.visit(this, context.next(false, queryServerInstance, workerId));
          } else {
            // only print short version of the sender node
            appendMailboxSend(sender, context.next(true, queryServerInstance, workerId))
                .append(" (Subtree Omitted)")
                .append('\n');
          }
        }
      }
    }
    return context._builder;
  }

  @Override
  public StringBuilder visitMailboxSend(MailboxSendNode node, Context context) {
    appendMailboxSend(node, context).append('\n');
    return node.getInputs().get(0).visit(this, context.next(false, context._host, context._workerId));
  }

  private StringBuilder appendMailboxSend(MailboxSendNode node, Context context) {
    appendInfo(node, context);

    int receiverStageId = node.getReceiverStageId();
    Map<QueryServerInstance, List<Integer>> servers = _queryPlan.getDispatchablePlanMetadataMap().get(receiverStageId)
        .getServerInstanceToWorkerIdMap();
    context._builder.append("->");
    String receivers = servers.entrySet().stream()
        .map(ExplainPlanStageVisitor::stringifyQueryServerInstanceToWorkerIdsEntry)
        .map(s -> "[" + receiverStageId + "]@" + s)
        .collect(Collectors.joining(",", "{", "}"));
    return context._builder.append(receivers);
  }

  @Override
  public StringBuilder visitProject(ProjectNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitSort(SortNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitTableScan(TableScanNode node, Context context) {
    return appendInfo(node, context)
        .append(' ')
        .append(_queryPlan.getDispatchablePlanMetadataMap()
            .get(node.getStageId())
            .getWorkerIdToSegmentsMap()
            .get(context._host))
        .append('\n');
  }

  @Override
  public StringBuilder visitValue(ValueNode node, Context context) {
    return appendInfo(node, context);
  }

  static class Context {
    final QueryServerInstance _host;
    final int _workerId;
    final String _prefix;
    final String _childPrefix;
    final StringBuilder _builder;

    Context(QueryServerInstance host, int workerId, String prefix, String childPrefix, StringBuilder builder) {
      _host = host;
      _workerId = workerId;
      _prefix = prefix;
      _childPrefix = childPrefix;
      _builder = builder;
    }

    Context next(boolean hasMoreChildren, QueryServerInstance host, int workerId) {
      return new Context(
          host,
          workerId,
          hasMoreChildren ? _childPrefix + "├── " : _childPrefix + "└── ",
          hasMoreChildren ? _childPrefix + "│   " : _childPrefix + "   ",
          _builder
      );
    }
  }

  public static String stringifyQueryServerInstanceToWorkerIdsEntry(Map.Entry<QueryServerInstance, List<Integer>> e) {
    return e.getKey() + "|" + e.getValue();
  }
}
