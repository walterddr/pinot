package org.apache.pinot.query.context;

import java.util.Map;
import org.apache.calcite.plan.Context;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * A wrapper around {@link Context}. Since Calcite's Context is not used, we can leverage this for passing plan options.
 */
public class PinotRelOptPlannerContext implements Context {
  public static final String USE_DYNAMIC_FILTER = "option_key__USE_DYNAMIC_FILTER";
  public static final String USE_HASH_DISTRIBUTE = "option_key__USE_HASH_DISTRIBUTE";
  public static final String USE_BROADCAST_DISTRIBUTE = "option_key__USE_BROADCAST_DISTRIBUTE";

  private final Map<String, String> _options;

  public PinotRelOptPlannerContext(Map<String, String> options) {
    _options = options;
  }

  @Override
  public <C> @Nullable C unwrap(Class<C> aClass) {
    return null;
  }

  public Map<String, String> getOptions() {
    return _options;
  }
}
