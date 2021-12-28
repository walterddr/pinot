package org.apache.pinot.query;

import com.google.common.base.Preconditions;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.transport.grpc.GrpcQueryServer;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryWorker extends PinotQueryWorkerGrpc.PinotQueryWorkerImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryServer.class);

  private final Server _server;
  private final QueryRunner _queryRunner;
  private final ExecutorService _executorService =
      Executors.newFixedThreadPool(ResourceManager.DEFAULT_QUERY_WORKER_THREADS);

  public QueryWorker(int port, QueryRunner queryRunner) {
    _server = ServerBuilder.forPort(port).addService(this).build();
    _queryRunner = queryRunner;
    LOGGER.info("Initialized QueryWorker on port: {} with numWorkerThreads: {}", port,
        ResourceManager.DEFAULT_QUERY_WORKER_THREADS);
  }

  public void start() {
    LOGGER.info("Starting QueryWorker");
    try {
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down QueryWorker");
    try {
      _server.shutdown().awaitTermination();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void submit(Worker.QueryRequest request, StreamObserver<Worker.QueryResponse> responseObserver) {
    // Deserialize the request
    WorkerQueryRequest workerQueryRequest;
    try (ByteArrayInputStream bs = new ByteArrayInputStream(request.toByteArray());
        ObjectInputStream is = new ObjectInputStream(bs)) {
      Object o = is.readObject();
      Preconditions.checkState(o instanceof WorkerQueryRequest, "invalid worker query request object");
      workerQueryRequest = (WorkerQueryRequest) o;
    } catch (Exception e) {
      LOGGER.error("Caught exception while deserializing the request: {}", request, e);
      responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Bad request").withCause(e).asException());
      return;
    }

    // return dispatch successful.
    responseObserver.onNext(Worker.QueryResponse.newBuilder().putMetadata("OK", "OK").build());
    responseObserver.onCompleted();

    // Process the query
    try {
      // TODO: break this into parsing and execution, so that responseObserver can return upon parsing complete.
      _queryRunner.processQuery(workerQueryRequest, _executorService);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing request", e);
      throw new RuntimeException(e);
    }
  }
}
