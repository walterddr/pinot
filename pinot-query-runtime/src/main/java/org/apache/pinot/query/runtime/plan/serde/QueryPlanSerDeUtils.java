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
package org.apache.pinot.query.runtime.plan.serde;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.plan.DistributedQueryPlan;


/**
 * This utility class serialize/deserialize between {@link Worker.QueryPlan} and {@link DistributedQueryPlan}.
 */
public class QueryPlanSerDeUtils {

  private QueryPlanSerDeUtils() {
    // do not instantiate.
  }

  public static DistributedQueryPlan deserialize(Worker.QueryPlan workerQueryPlan) {
    DistributedQueryPlan distributedQueryPlan = new DistributedQueryPlan(workerQueryPlan.getStageId());
    distributedQueryPlan.setServerInstance(stringToInstance(workerQueryPlan.getInstanceId()));
    distributedQueryPlan.setStageRoot(stagePlanToStageRoot(workerQueryPlan.getStagePlan()));
    Map<Integer, StageMetadata> metadataMap = distributedQueryPlan.getMetadataMap();
    for (Map.Entry<Integer, Worker.StageMetadata> e : workerQueryPlan.getStageMetadataMap().entrySet()) {
      metadataMap.put(e.getKey(), fromWorkerStageMetadata(e.getValue()));
    }
    return distributedQueryPlan;
  }

  public static Worker.QueryPlan serialize(DistributedQueryPlan distributedQueryPlan) {
    return Worker.QueryPlan.newBuilder().setStageId(distributedQueryPlan.getStageId())
        .setInstanceId(instanceToString(distributedQueryPlan.getServerInstance()))
        .setStagePlan(stageRootToStagePlan(distributedQueryPlan.getStageRoot()))
        .putAllStageMetadata(constructStageMetadataMap(distributedQueryPlan.getMetadataMap())).build();
  }

  private static ServerInstance stringToInstance(String serverInstanceString) {
    String[] s = StringUtils.split(serverInstanceString, '_');
    return new WorkerInstance(s[0], Integer.parseInt(s[1]), Integer.parseInt(s[2]));
  }

  private static String instanceToString(ServerInstance serverInstance) {
    return StringUtils.join(serverInstance.getHostname(), '_', serverInstance.getPort(), '_',
        serverInstance.getGrpcPort());
  }

  private static Worker.StagePlan stageRootToStagePlan(StageNode stageRoot) {
    try (ByteArrayOutputStream bs = new ByteArrayOutputStream(); ObjectOutputStream os = new ObjectOutputStream(bs)) {
      os.writeObject(stageRoot);
      return Worker.StagePlan.newBuilder().setSerializedStagePlan(ByteString.copyFrom(bs.toByteArray())).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static StageNode stagePlanToStageRoot(Worker.StagePlan stagePlan) {
    try (ByteArrayInputStream bs = new ByteArrayInputStream(stagePlan.getSerializedStagePlan().toByteArray());
        ObjectInputStream is = new ObjectInputStream(bs)) {
      Object o = is.readObject();
      Preconditions.checkState(o instanceof StageNode, "invalid worker query request object");
      return (StageNode) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static StageMetadata fromWorkerStageMetadata(Worker.StageMetadata workerStageMetadata) {
    StageMetadata stageMetadata = new StageMetadata();
    stageMetadata.getScannedTables().addAll(workerStageMetadata.getScannedTablesList());
    for (String serverInstanceString : workerStageMetadata.getInstancesList()) {
      stageMetadata.getServerInstances().add(stringToInstance(serverInstanceString));
    }
    for (Map.Entry<String, Worker.SegmentMetadata> e : workerStageMetadata.getSegmentMetadataMap().entrySet()) {
      stageMetadata.getServerInstanceToSegmentsMap().put(stringToInstance(e.getKey()), e.getValue().getSegmentsList());
    }
    return stageMetadata;
  }

  private static Worker.StageMetadata toWorkerStageMetadata(StageMetadata stageMetadata) {
    Worker.StageMetadata.Builder builder = Worker.StageMetadata.newBuilder();
    builder.addAllScannedTables(stageMetadata.getScannedTables());
    for (ServerInstance serverInstance : stageMetadata.getServerInstances()) {
      builder.addInstances(instanceToString(serverInstance));
    }
    for (Map.Entry<ServerInstance, List<String>> e : stageMetadata.getServerInstanceToSegmentsMap().entrySet()) {
      builder.putSegmentMetadata(instanceToString(e.getKey()),
          Worker.SegmentMetadata.newBuilder().addAllSegments(e.getValue()).build());
    }
    return builder.build();
  }

  private static Map<Integer, Worker.StageMetadata> constructStageMetadataMap(Map<Integer, StageMetadata> metadataMap) {
    Map<Integer, Worker.StageMetadata> protoMap = new HashMap<>();
    for (Map.Entry<Integer, StageMetadata> e : metadataMap.entrySet()) {
      protoMap.put(e.getKey(), toWorkerStageMetadata(e.getValue()));
    }
    return protoMap;
  }
}
