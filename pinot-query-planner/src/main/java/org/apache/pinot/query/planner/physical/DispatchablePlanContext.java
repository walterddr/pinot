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
package org.apache.pinot.query.planner.physical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.util.Pair;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.routing.WorkerManager;


public class DispatchablePlanContext {
  private final WorkerManager _workerManager;

  private final long _requestId;
  private final Set<String> _tableNames;
  private final List<Pair<Integer, String>> _resultFields;

  private final PlannerContext _plannerContext;
  private final Map<Integer, DispatchablePlanMetadata> _dispatchablePlanMetadataMap;
  private final Map<Integer, StageNode> _dispatchablePlanStageRootMap;


  public DispatchablePlanContext(WorkerManager workerManager, long requestId, PlannerContext plannerContext,
      List<Pair<Integer, String>> resultFields, Set<String> tableNames) {
    _workerManager = workerManager;
    _requestId = requestId;
    _plannerContext = plannerContext;
    _dispatchablePlanMetadataMap = new HashMap<>();
    _dispatchablePlanStageRootMap = new HashMap<>();
    _resultFields = resultFields;
    _tableNames = tableNames;
  }

  public WorkerManager getWorkerManager() {
    return _workerManager;
  }

  public long getRequestId() {
    return _requestId;
  }

  // Returns all the table names.
  public Set<String> getTableNames() {
    return _tableNames;
  }

  public List<Pair<Integer, String>> getResultFields() {
    return _resultFields;
  }

  public PlannerContext getPlannerContext() {
    return _plannerContext;
  }

  public Map<Integer, DispatchablePlanMetadata> getDispatchablePlanMetadataMap() {
    return _dispatchablePlanMetadataMap;
  }

  public Map<Integer, StageNode> getDispatchablePlanStageRootMap() {
    return _dispatchablePlanStageRootMap;
  }
}
