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
package org.apache.pinot.query.planner.nodes;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.type.RelDataType;


public class MailboxSendNode extends AbstractStageNode {
  private int _receiverStageId;
  private RelDistribution.Type _exchangeType;

  public MailboxSendNode(int stageId) {
    super(stageId, null);
  }

  public MailboxSendNode(int stageId, RelDataType rowType, int receiverStageId, RelDistribution.Type exchangeType) {
    super(stageId, rowType);
    _receiverStageId = receiverStageId;
    _exchangeType = exchangeType;
  }

  public int getReceiverStageId() {
    return _receiverStageId;
  }

  public RelDistribution.Type getExchangeType() {
    return _exchangeType;
  }
}
