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
package org.apache.pinot.query.runtime.plan;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;


/**
 * {@code NodeFinderVisitor} finds all nodes that matches the node type
 */
@SuppressWarnings("unchecked")
public class NodeFinderVisitor implements StageNodeVisitor<List<StageNode>, Class<? extends StageNode>> {
  private static final NodeFinderVisitor INSTANCE = new NodeFinderVisitor();

  public static List<StageNode> find(StageNode root, Class<? extends StageNode> nodeClazz) {
    return root.visit(INSTANCE, nodeClazz);
  }

  @Override
  public List<StageNode> visitAggregate(AggregateNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitFilter(FilterNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitJoin(JoinNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitMailboxReceive(MailboxReceiveNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitMailboxSend(MailboxSendNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitProject(ProjectNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitSort(SortNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitTableScan(TableScanNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitValue(ValueNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  @Override
  public List<StageNode> visitWindow(WindowNode node, Class<? extends StageNode> clazz) {
    return visitChildren(node, clazz);
  }

  private List<StageNode> visitChildren(StageNode node, Class<? extends StageNode> clazz) {
    List<StageNode> result = new ArrayList<>();
    for (StageNode child : node.getInputs()) {
      List<StageNode> childResult = child.visit(this, clazz);
      result.addAll(childResult);
    }
    if (clazz == node.getClass()) {
      result.add(node);
    }
    return result;
  }
}
