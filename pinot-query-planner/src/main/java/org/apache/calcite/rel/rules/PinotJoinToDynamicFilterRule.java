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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.query.context.PinotRelOptPlannerContext;


/**
 * Special rule for Pinot, this rule transposing an INNER JOIN into dynamic filter for the leaf stage.
 *
 * <p>Consider the following INNER JOIN plan
 *
 *                  ...                                     ...
 *                   |                                       |
 *             [ Transform ]                           [ Transform ]
 *                   |                                       |
 *             [ Inner Join ]                          [ Passthrough ]
 *             /            \                          /
 *        [xChange]      [xChange]                [xChange]
 *           /                \                      /
 *     [ Transform ]     [ Transform ]         [ Transform ]
 *          |                  |                    |
 *     [Proj/Filter]     [Proj/Filter]         [Proj/Filter] <-----
 *          |                  |                    |              \
 *     [Table Scan ]     [Table Scan ]         [Table Scan ]    [xChange]
 *                                                                   \
 *                                                              [ Transform ]
 *                                                                    |
 *                                                              [Proj/Filter]
 *                                                                    |
 *                                                              [Table Scan ]
 *
 * <p> This rule is one part of the overall mechanism and this rule only does the following
 *
 *                 ...                                       ...
 *                  |                                         |
 *            [ Transform ]                             [ Transform ]
 *                  |                                         |
 *            [ Inner Join ]                           [ Pass-through ]
 *            /            \                            /
 *       [xChange]      [xChange]                  [xChange]
 *          /                \                        /
 *    [ Transform ]     [ Transform ]           [Dyn. Filter] <-----
 *         |                  |                      |              \
 *    [Proj/Filter]     [Proj/Filter]           [ Transform ]    [xChange]
 *         |                  |                      |                \
 *    [Table Scan ]     [Table Scan ]           [Proj/Filter]    [ Transform ]
 *                                                   |                 |
 *                                              [Table Scan ]    [Proj/Filter]
 *                                                                     |
 *                                                               [Table Scan ]
 *
 * <p> The next part to compile the Dynamic filter into the Proj/Filter operator happens in the runtime.
 *
 * <p> This rewrite is only useful if we can ensure that:
 * <ul>
 *   <li>new plan can leverage the indexes and fast filtering of the leaf-stage Pinot server processing logic.</li>
 *   <li>
 *     data sending over xChange should be relatively small comparing to data would've been selected out if the dynamic
 *     filter is not applied. E.g. selectivity of the dynamic filter should out-weight the data transfer overhead.
 *   </li>
 *   <li>
 *     since leaf-stage operator can only process a typical Pinot V1 engine chain-operator chain. This means the entire
 *     right-table will be ship over and persist in memory before the dynamic filter can occur. memory foot print will
 *     be high so this rule should be use carefully until we have cost-based optimization.
 *   </li>
 * </ul>
 */
public class PinotJoinToDynamicFilterRule extends RelOptRule {
  public static final PinotJoinToDynamicFilterRule INSTANCE =
      new PinotJoinToDynamicFilterRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinToDynamicFilterRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    PinotRelOptPlannerContext context = (PinotRelOptPlannerContext) call.getPlanner().getContext();
    if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      JoinInfo joinInfo = join.analyzeCondition();
      RelNode left = join.getLeft() instanceof HepRelVertex ? ((HepRelVertex) join.getLeft()).getCurrentRel()
          : join.getLeft();
      RelNode right = join.getRight() instanceof HepRelVertex ? ((HepRelVertex) join.getRight()).getCurrentRel()
          : join.getRight();
      return join.getJoinType() == JoinRelType.SEMI && CollectionUtils.isEmpty(joinInfo.nonEquiConditions)
          && left instanceof LogicalExchange && right instanceof LogicalExchange
          && PinotRuleUtils.noExchangeInSubtree(left.getInput(0))
          && context.getOptions().containsKey(PinotRelOptPlannerContext.USE_DYNAMIC_FILTER);
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    LogicalExchange left = (LogicalExchange) (join.getLeft() instanceof HepRelVertex
        ? ((HepRelVertex) join.getLeft()).getCurrentRel() : join.getLeft());
    LogicalExchange right = (LogicalExchange) (join.getRight() instanceof HepRelVertex
        ? ((HepRelVertex) join.getRight()).getCurrentRel() : join.getRight());

    LogicalExchange broadcastDynamicFilterExchange = LogicalExchange.create(right.getInput(),
        RelDistributions.BROADCAST_DISTRIBUTED);
    Join dynamicFilterJoin =
        new LogicalJoin(join.getCluster(), join.getTraitSet(), left.getInput(), broadcastDynamicFilterExchange,
            join.getCondition(), join.getVariablesSet(), JoinRelType.SEMI, join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()));
    LogicalExchange passThroughAfterJoinExchange =
        LogicalExchange.create(dynamicFilterJoin, RelDistributions.SINGLETON);
    call.transformTo(passThroughAfterJoinExchange);
  }
}
