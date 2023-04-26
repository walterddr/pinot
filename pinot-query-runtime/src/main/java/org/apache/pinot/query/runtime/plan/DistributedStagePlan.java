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
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;


/**
 * {@code DistributedStagePlan} is the deserialized version of the
 * {@link org.apache.pinot.common.proto.Worker.StagePlan}.
 *
 * <p>It is also the extended version of the {@link org.apache.pinot.core.query.request.ServerQueryRequest}.
 */
public class DistributedStagePlan {
  private int _stageId;
  private VirtualServerAddress _serverAddress;
  private StageNode _stageRoot;
  private List<StageMetadata> _stageMetadataList;

  public DistributedStagePlan(int stageId) {
    _stageId = stageId;
    _stageMetadataList = new ArrayList<>();
  }

  public DistributedStagePlan(int stageId, VirtualServerAddress serverAddress, StageNode stageRoot,
      List<StageMetadata> stageMetadataList) {
    _stageId = stageId;
    _serverAddress = serverAddress;
    _stageRoot = stageRoot;
    _stageMetadataList = stageMetadataList;
  }

  public int getStageId() {
    return _stageId;
  }

  public VirtualServerAddress getServer() {
    return _serverAddress;
  }

  public StageNode getStageRoot() {
    return _stageRoot;
  }

  public List<StageMetadata> getStageMetadataList() {
    return _stageMetadataList;
  }

  public void setServer(VirtualServerAddress serverAddress) {
    _serverAddress = serverAddress;
  }

  public void setStageRoot(StageNode stageRoot) {
    _stageRoot = stageRoot;
  }

  public StageMetadata getCurrentStageMetadata() {
    return _stageMetadataList.get(_stageId);
  }

  public WorkerMetadata getCurrentWorkerMetadata() {
    return getCurrentStageMetadata().getWorkerMetadataList().get(_serverAddress.workerId());
  }
}
