package io.taskscheduler;

/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.errorprone.annotations.ForOverride;
import io.grpc.Status;
import io.grpc.taskscheduler.*;

import io.taskscheduler.SessionManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.logging.Logger;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class TaskScheduler {
    private static final int PORT = 50051;
    private static final Logger logger = Logger.getLogger(TaskSchedulerGrpc.class.getName());

    private Server server;


    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new TaskScheduler.TaskSchedulerImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                io.taskscheduler.TaskScheduler.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final TaskScheduler server = new TaskScheduler();
        server.start();
        server.blockUntilShutdown();
    }

    static class TaskSchedulerImpl extends TaskSchedulerGrpc.TaskSchedulerImplBase {
        private final SessionManager sessions = new SessionManager(1000);

        /**
         * <pre>
         *
         * </pre>
         */
        @Override
        public void registerWorker(io.grpc.taskscheduler.RegistrationRequest request,
                                   io.grpc.stub.StreamObserver<io.grpc.taskscheduler.RegistrationReply> responseObserver) {
            String sessionId = sessions.createSession().id;

            RegistrationReply reply = RegistrationReply.newBuilder().setAuthToken(sessionId.toString()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        /**
         */
        @Override
        public void getTask(io.grpc.taskscheduler.RequestContext request,
                            io.grpc.stub.StreamObserver<io.grpc.taskscheduler.Task> responseObserver) {
            /**
             * Validate session
             * TODO: Tranform it in a middleware
             */
            Session session = sessions.retrieveSession(request.getAuthToken());
            if (session == null) {
                // error out
                responseObserver.onError(Status
                        .UNAUTHENTICATED
                        .withDescription("Invalid session token provided")
                        .asRuntimeException());
                return;
            }

            // return transfer orders
            TransferOrder order1 = TransferOrder.newBuilder().setId(1).build();

            TransferOrder order2 = TransferOrder
                    .newBuilder()
                    .setId(2)
                    .build();

            Task reply = Task.newBuilder()
                    .addOrders(order1)
                    .addOrders(order2)
                    .build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        /**
         */
        @Override
        public void sendHeartBeat(io.grpc.taskscheduler.CallerData request,
                              io.grpc.stub.StreamObserver<io.grpc.taskscheduler.HeartBeatReply> responseObserver) {
            /**
             * Validate session
             * TODO: Transform it in a middleware
             */
            Session session = sessions.retrieveSession(request.getContext().getAuthToken());
            if (session == null) {
                // error out
                responseObserver.onError(Status.UNAUTHENTICATED
                        .withDescription("Invalid session token provided")
                        .asRuntimeException());
                return;
            }

            // Record stats
            long tasksCompleted = request.getTaskCompleted();
            long tasksReceived = request.getTaskReceived();
            long tasksFailed = tasksCompleted - tasksReceived;
            LocalDateTime time = LocalDateTime.now();

            session.addStats(time, tasksReceived, tasksCompleted, tasksFailed);

            HeartBeatReply reply = HeartBeatReply.newBuilder().build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
