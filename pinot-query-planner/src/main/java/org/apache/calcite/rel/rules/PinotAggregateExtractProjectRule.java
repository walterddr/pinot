package org.apache.calcite.rel.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;


public class PinotAggregateExtractProjectRule {
  public static final AggregateExtractProjectRule INSTANCE =
      new AggregateExtractProjectRule(ImmutableAggregateExtractProjectRule.Config.of()
          .withOperandSupplier(b0 -> b0.operand(Aggregate.class).oneInput(
              b1 -> b1.operand(RelNode.class).predicate(r -> !(r instanceof Project)).anyInputs()))
          .withRelBuilderFactory(PinotRuleUtils.PINOT_REL_FACTORY));
}
