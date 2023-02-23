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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.PinotHintUtils;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, this rule transposing an INNER JOIN into dynamic broadcast join for the leaf stage.
 *
 * <p>Consider the following INNER JOIN plan
 *
 *                  ...                                     ...
 *                   |                                       |
 *             [ Transform ]                           [ Transform ]
 *                   |                                       |
 *             [ Inner Join ]                     [ Pass-through xChange ]
 *             /            \                          /
 *        [xChange]      [xChange]              [Inner Join] <----
 *           /                \                      /            \
 *     [ Transform ]     [ Transform ]         [ Transform ]       \
 *          |                  |                    |               \
 *     [Proj/Filter]     [Proj/Filter]         [Proj/Filter]  <------\
 *          |                  |                    |                 \
 *     [Table Scan ]     [Table Scan ]         [Table Scan ]       [xChange]
 *                                                                      \
 *                                                                 [ Transform ]
 *                                                                       |
 *                                                                 [Proj/Filter]
 *                                                                       |
 *                                                                 [Table Scan ]
 *
 * <p> This rule is one part of the overall mechanism and this rule only does the following
 *
 *                 ...                                      ...
 *                  |                                        |
 *            [ Transform ]                             [ Transform ]
 *                  |                                       /
 *            [ Inner Join ]                     [Pass-through xChange]
 *            /            \                            /
 *       [xChange]      [xChange]          |----[Dyn. Broadcast]   <-----
 *          /                \             |           |                 \
 *    [ Transform ]     [ Transform ]      |----> [ Inner Join ]      [xChange]
 *         |                  |            |           |                   \
 *    [Proj/Filter]     [Proj/Filter]      |      [ Transform ]       [ Transform ]
 *         |                  |            |          |                    |
 *    [Table Scan ]     [Table Scan ]      |----> [Proj/Filter]       [Proj/Filter]
 *                                                     |                    |
 *                                                [Table Scan ]       [Table Scan ]
 *
 *
 *
 * <p> The next part to extend the Dynamic broadcast into the Proj/Filter operator happens in the runtime.
 *
 * <p> This rewrite is only useful if we can ensure that:
 * <ul>
 *   <li>new plan can leverage the indexes and fast filtering of the leaf-stage Pinot server processing logic.</li>
 *   <li>
 *     data sending over xChange should be relatively small comparing to data would've been selected out if the dynamic
 *     broadcast is not applied.
 *   </li>
 *   <li>
 *     since leaf-stage operator can only process a typical Pinot V1 engine chain-operator chain. This means the entire
 *     right-table will be ship over and persist in memory before the dynamic broadcast can occur. memory foot print
 *     will be high so this rule should be use carefully until we have cost-based optimization.
 *   </li>
 *   <li>
 *     if the dynamic broadcast stage is broadcasting to a left-table that's pre-partitioned with the join key, then the
 *     right-table will be ship over and persist in memory using hash distribution. memory foot print will be
 *     relatively low comparing to non-partitioned.
 *   </li>
 *   <li>
 *     if the dynamic broadcast stage operating on a table with the same partition scheme as the left-table, then there
 *     is not going to be any network shuffling overhead. both memory and latency overhead will be low.
 *   </li>
 * </ul>
 */
public class PinotJoinToDynamicBroadcastRule extends RelOptRule {
  public static final PinotJoinToDynamicBroadcastRule INSTANCE =
      new PinotJoinToDynamicBroadcastRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinToDynamicBroadcastRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1 || !(call.rel(0) instanceof Join)) {
      return false;
    }
    Join join = call.rel(0);
    Set<String> joinStrategyStrings = PinotHintUtils.getJoinStrategyStrings(join.getHints());
    if (!joinStrategyStrings.contains("dynamic_filter")) {
      return false;
    }
    JoinInfo joinInfo = join.analyzeCondition();
    RelNode left = join.getLeft() instanceof HepRelVertex ? ((HepRelVertex) join.getLeft()).getCurrentRel()
        : join.getLeft();
    RelNode right = join.getRight() instanceof HepRelVertex ? ((HepRelVertex) join.getRight()).getCurrentRel()
        : join.getRight();
    return left instanceof LogicalExchange && right instanceof LogicalExchange
        && PinotRuleUtils.noExchangeInSubtree(left.getInput(0))
        && (join.getJoinType() == JoinRelType.INNER
            || (join.getJoinType() == JoinRelType.SEMI && joinInfo.nonEquiConditions.isEmpty()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    LogicalExchange left = (LogicalExchange) (join.getLeft() instanceof HepRelVertex
        ? ((HepRelVertex) join.getLeft()).getCurrentRel() : join.getLeft());
    LogicalExchange right = (LogicalExchange) (join.getRight() instanceof HepRelVertex
        ? ((HepRelVertex) join.getRight()).getCurrentRel() : join.getRight());

    LogicalExchange broadcastDynamicFilterExchange;
    Set<String> joinStrategyStrings = PinotHintUtils.getJoinStrategyStrings(join.getHints());
    if (joinStrategyStrings.contains("colocated")) {
      broadcastDynamicFilterExchange = LogicalExchange.create(right.getInput(),
          RelDistributions.SINGLETON);
    } else {
      broadcastDynamicFilterExchange = LogicalExchange.create(right.getInput(),
          RelDistributions.BROADCAST_DISTRIBUTED);
    }
    Join dynamicFilterJoin =
        new LogicalJoin(join.getCluster(), join.getTraitSet(), left.getInput(), broadcastDynamicFilterExchange,
            join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()));
    LogicalExchange passThroughAfterJoinExchange =
        LogicalExchange.create(dynamicFilterJoin, RelDistributions.SINGLETON);
    call.transformTo(passThroughAfterJoinExchange);
  }
}
