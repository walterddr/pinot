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
package org.apache.pinot.broker.routing.segmentpartition;

import java.util.Set;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


public class SegmentPartitionInfo {
  private final String _partitionColumn;
  private final int _numPartitions;
  private final PartitionFunction _partitionFunction;
  private final Set<Integer> _partitions;

  public SegmentPartitionInfo(String partitionColumn, int numPartitions, PartitionFunction partitionFunction,
      Set<Integer> partitions) {
    _partitionColumn = partitionColumn;
    _numPartitions = numPartitions;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
  }

  public int getNumPartitions() {
    return _numPartitions;
  }

  public String getPartitionColumn() {
    return _partitionColumn;
  }

  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  public Set<Integer> getPartitions() {
    return _partitions;
  }
}
