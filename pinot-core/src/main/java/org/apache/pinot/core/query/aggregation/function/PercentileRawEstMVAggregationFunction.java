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
package org.apache.pinot.core.query.aggregation.function;

import org.apache.pinot.spi.request.context.context.ExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The {@code PercentileRawEstMVAggregationFunction} returns the serialized {@code QuantileDigest} data structure of the
 * {@code PercentileEstMVAggregationFunction}.
 */
public class PercentileRawEstMVAggregationFunction extends PercentileRawEstAggregationFunction {

  public PercentileRawEstMVAggregationFunction(ExpressionContext expressionContext, int percentile) {
    super(expressionContext, new PercentileEstMVAggregationFunction(expressionContext, percentile));
  }

  public PercentileRawEstMVAggregationFunction(ExpressionContext expressionContext, double percentile) {
    super(expressionContext, new PercentileEstMVAggregationFunction(expressionContext, percentile));
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILERAWESTMV;
  }
}
