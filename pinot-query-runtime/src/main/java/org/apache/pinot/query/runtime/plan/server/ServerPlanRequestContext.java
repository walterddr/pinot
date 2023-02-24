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
package org.apache.pinot.query.runtime.plan.server;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.PlanRequestContext;
import org.apache.pinot.spi.config.table.TableType;


/**
 * Context class for converting a {@link org.apache.pinot.query.runtime.plan.DistributedStagePlan} into
 * {@link PinotQuery} to execute on server.
 */
public class ServerPlanRequestContext extends PlanRequestContext {
  public static final String EMPTY_RETURN_HINT = "constant_empty_return";

  protected TableType _tableType;
  protected TimeBoundaryInfo _timeBoundaryInfo;

  protected PinotQuery _pinotQuery;
  protected TransferableBlock _dynamicMailboxResultBlock;
  protected InstanceRequest _instanceRequest;

  protected Set<String> _hints;

  public ServerPlanRequestContext(MailboxService<TransferableBlock> mailboxService, long requestId, int stageId,
      long timeoutMs, VirtualServerAddress server, Map<Integer, StageMetadata> metadataMap, PinotQuery pinotQuery,
      TableType tableType, TimeBoundaryInfo timeBoundaryInfo, TransferableBlock dynamicMailboxResultBlock) {
    super(mailboxService, requestId, stageId, timeoutMs, server, metadataMap);
    _hints = new HashSet<>();
    _pinotQuery = pinotQuery;
    _tableType = tableType;
    _timeBoundaryInfo = timeBoundaryInfo;
    _dynamicMailboxResultBlock = dynamicMailboxResultBlock;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public PinotQuery getPinotQuery() {
    return _pinotQuery;
  }

  public InstanceRequest getInstanceRequest() {
    return _instanceRequest;
  }

  public TransferableBlock getDynamicOperatorResult() {
    return _dynamicMailboxResultBlock;
  }

  public void setInstanceRequest(InstanceRequest instanceRequest) {
    _instanceRequest = instanceRequest;
  }

  // Only hints are modifiable for PlanRequestContext

  public void addHints(String hint) {
    _hints.add(hint);
  }

  public Set<String> getHints() {
    return _hints;
  }
}
