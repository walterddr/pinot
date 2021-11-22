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
package org.apache.pinot.broker.requesthandler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The <code>SingleConnectionBrokerRequestHandler</code> class is a thread-safe broker request handler using a single
 * connection per server to route the queries.
 */
@ThreadSafe
public class GrpcBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final int DEFAULT_MAXIMUM_REQUEST_ATTEMPT = 10;

  private final GrpcQueryClient.Config _grpcConfig;
  private final Map<String, AtomicInteger> _concurrentQueriesCountMap = new ConcurrentHashMap<>();
  private final int _maxBacklogPerServer;
  private final int _grpcBrokerThreadPoolSize;

  private transient PinotStreamingQueryClient _streamingQueryClient;

  public GrpcBrokerRequestHandler(PinotConfiguration config, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics, TlsConfig tlsConfig) {
    super(config, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics);
    _grpcConfig = buildGrpcQueryClientConfig(config);

    // parse and fill constants
    _maxBacklogPerServer = Integer.parseInt(_config.getProperty("MAXIMUM_BACKLOG_PER_SERVER", "1000"));
    _grpcBrokerThreadPoolSize = Integer.parseInt(_config.getProperty("GRPC_BROKER_THREAD_POOL_SIZE", "10"));

    // create streaming query client
    _streamingQueryClient = new PinotStreamingQueryClient(_grpcConfig);
  }

  @Override
  public void start() {
  }

  @Override
  public synchronized void shutDown() {
    _brokerReduceService.shutDown();
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<ServerInstance, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable,
      long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics)
      throws Exception {
    assert offlineBrokerRequest != null && offlineRoutingTable != null;

    String rawTableName = TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());
    // Making request to a streaming server response.
    // TODO: support realtime as well. this is for testing purpose.
    Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> serverResponses =
        streamingQueryToPinotServer(requestId, _brokerId, rawTableName, TableType.OFFLINE,
            offlineBrokerRequest, offlineRoutingTable, timeoutMs, true, 1);
    BrokerResponseNative brokerResponse =
        _brokerReduceService.reduceOnStreamingServerResponses(
            originalBrokerRequest, serverResponses, timeoutMs, _brokerMetrics);

    return brokerResponse;
  }

//  private static <T> T doWithRetries(int retries, Function<Integer, T> caller)
//      throws RuntimeException {
//    ProcessingException firstError = null;
//    for (int i = 0; i < retries; i++) {
//      try {
//        return caller.apply(retries);
//      } catch (Exception e) {
//        if (e.getCause() instanceof ProcessingException
//            && isRetriable(((ProcessingException) e.getCause()).getErrorCode())) {
//          if (firstError == null) {
//            firstError = (ProcessingException) e.getCause();
//          }
//        } else {
//          throw e;
//        }
//      }
//    }
//    throw new RuntimeException(firstError);
//  }
//
//  // return false for now on all exception.
//  private static boolean isRetriable(int errorCode) {
//    return false;
//  }

  /**
   * Query pinot server for data table.
   */
  public Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> streamingQueryToPinotServer(
      final long requestId,
      final String brokerHost,
      final String rawTableName,
      final TableType tableType,
      BrokerRequest brokerRequest,
      Map<ServerInstance, List<String>> routingTable,
      long connectionTimeoutInMillis,
      boolean ignoreEmptyResponses,
      int pinotRetryCount) {
    // Retries will all hit the same server because the routing decision has already been made by the pinot broker
    Map<ServerRoutingInstance, Iterator<Server.ServerResponse>> serverResponseMap = new HashMap<>();
    for (Map.Entry<ServerInstance, List<String>> routingEntry : routingTable.entrySet()) {
      ServerInstance serverInstance = routingEntry.getKey();
      List<String> segments = routingEntry.getValue();
      String serverHost = serverInstance.getHostname();
//      int port = serverInstance.getPort();
      int port = CommonConstants.Server.DEFAULT_GRPC_PORT;
      // ensure concurrent request count from the same grpc server cannot exceed maximum
      if (!_concurrentQueriesCountMap.containsKey(serverHost)) {
        _concurrentQueriesCountMap.put(serverHost, new AtomicInteger(0));
      }
      int concurrentQueryNum = _concurrentQueriesCountMap.get(serverHost).get();
      if (concurrentQueryNum > _maxBacklogPerServer) {
        ProcessingException processingException = new ProcessingException(QueryException.UNKNOWN_ERROR_CODE);
        processingException.setMessage("Reaching server query max backlog size is - " + _maxBacklogPerServer);
        throw new RuntimeException(processingException);
      }
//      _concurrentQueriesCountMap.get(serverHost).incrementAndGet();
      Iterator<Server.ServerResponse> streamingResponse = _streamingQueryClient.submit(serverHost, port,
          new GrpcRequestBuilder()
              .setSegments(segments)
              .setBrokerRequest(brokerRequest)
              .setEnableStreaming(true));
      serverResponseMap.put(serverInstance.toServerRoutingInstance(tableType), streamingResponse);
    }
    return serverResponseMap;
  }

  private static long makeGrpcRequestId(long requestId, int requestAttemptId) {
    return requestId * DEFAULT_MAXIMUM_REQUEST_ATTEMPT + requestAttemptId;
  }

  // return empty config for now
  private GrpcQueryClient.Config buildGrpcQueryClientConfig(PinotConfiguration config) {
    return new GrpcQueryClient.Config();
  }

  public static class PinotStreamingQueryClient {
    private final Map<String, GrpcQueryClient> _grpcQueryClientMap = new HashMap<>();
    private final GrpcQueryClient.Config _config;

    public PinotStreamingQueryClient(GrpcQueryClient.Config config) {
      _config = config;
    }

    public Iterator<Server.ServerResponse> submit(String host, int port, GrpcRequestBuilder requestBuilder) {
      GrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
      return client.submit(requestBuilder.build());
    }

    private GrpcQueryClient getOrCreateGrpcQueryClient(String host, int port) {
      String key = String.format("%s_%d", host, port);
      if (!_grpcQueryClientMap.containsKey(key)) {
        _grpcQueryClientMap.put(key, new GrpcQueryClient(host, port, _config));
      }
      return _grpcQueryClientMap.get(key);
    }
  }
}
