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
package org.apache.calcite.rel.hint;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


public class PinotHintUtils {

  private PinotHintUtils() {
    // do not instantiate.
  }

  public static boolean isAggFinalStage(Collection<RelHint> hints) {
    return hints.stream().map(h -> h.hintName).collect(Collectors.toSet()).contains(
        PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE);
  }

  public static boolean isAggIntermediateStage(Collection<RelHint> hints) {
    return hints.stream().map(h -> h.hintName).collect(Collectors.toSet()).contains(
        PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE);
  }

  public static Set<String> getJoinStrategyStrings(Collection<RelHint> hints) {
    Set<String> joinStrategyStrings = new HashSet<>();
    for (RelHint hint : hints) {
      if (PinotHintStrategyTable.JOIN_STRATEGY_HINT_KEY.equals(hint.hintName) && hint.listOptions != null) {
        joinStrategyStrings.addAll(hint.listOptions);
      }
    }
    return joinStrategyStrings;
  }
}
