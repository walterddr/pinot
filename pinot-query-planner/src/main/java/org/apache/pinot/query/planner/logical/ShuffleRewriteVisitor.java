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
package org.apache.pinot.query.planner.logical;

import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.context.PlannerContext;
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


public class ShuffleRewriteVisitor implements StageNodeVisitor<Boolean, PlannerContext> {

  /**
   * This method rewrites {@code root} <b>in place</b>, removing any unnecessary shuffles
   * by replacing the distribution type with {@link RelDistribution.Type#SINGLETON}.
   *
   * @param root the root node of the tree to rewrite
   */
  public static void optimizeShuffles(StageNode root, PlannerContext context) {
    root.visit(new ShuffleRewriteVisitor(), context);
  }

  private ShuffleRewriteVisitor() {
  }

  @Override
  public Boolean visitAggregate(AggregateNode node, PlannerContext context) {
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Boolean visitFilter(FilterNode node, PlannerContext context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Boolean visitJoin(JoinNode node, PlannerContext context) {
    Boolean leftRes = node.getInputs().get(0).visit(this, context);
    Boolean rightRes = node.getInputs().get(1).visit(this, context);

    return leftRes && rightRes;
  }

  @Override
  public Boolean visitMailboxReceive(MailboxReceiveNode node, PlannerContext context) {
    if (node.getSender().visit(this, context)) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
    }
    return false;
  }

  @Override
  public Boolean visitMailboxSend(MailboxSendNode node, PlannerContext context) {
    if (node.getInputs().get(0).visit(this, context)) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Boolean visitProject(ProjectNode node, PlannerContext context) {
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Boolean visitSort(SortNode node, PlannerContext context) {
    // sort doesn't change the partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Boolean visitTableScan(TableScanNode node, PlannerContext context) {
    // TODO: add table partition in table config as partition keys - this info is not yet available
    return context.getOptions().containsKey("skipShuffle");
  }

  @Override
  public Boolean visitValue(ValueNode node, PlannerContext context) {
    return false;
  }
}
