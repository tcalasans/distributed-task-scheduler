package io.taskscheduler;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import io.grpc.taskscheduler.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

public class TaskWorker {
    private static final String HOST = "localhost";
    private static final int PORT = 50051;

    private static final String SECRET_KEY = "BLABLABLA";
    private static final Logger logger = Logger.getLogger(io.taskscheduler.TaskWorker.class.getName());

    private final ManagedChannel channel;
    private final TaskSchedulerGrpc.TaskSchedulerBlockingStub blockingStub;
    private volatile String sessionToken = "";

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public TaskWorker(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    /** Construct client for accessing HelloWorld server using the existing channel. */
    TaskWorker(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = TaskSchedulerGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void registerWorker() {
        logger.info("Will try to register worker");
        RegistrationRequest request = RegistrationRequest.newBuilder().setKey(SECRET_KEY).build();
        RegistrationReply response;
        try {
            response = blockingStub.registerWorker(request);
            sessionToken = response.getAuthToken();
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            throw  e;
        }
        logger.info("Registration succeeded. Auth Token: " + response.getAuthToken());
    }

    public void sendHeartBeat() throws UnauthenticatedError {
        logger.info("Will try to send heart beat");
        CallerData request = CallerData
                .newBuilder()
                .setContext(RequestContext.newBuilder().setAuthToken(sessionToken).build())
                .setTaskReceived(20)
                .setTaskCompleted(20)
                .build();

        HeartBeatReply response;
        try {
            response = blockingStub.sendHeartBeat(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            if (e.getStatus() == Status.UNAUTHENTICATED) {
                throw new UnauthenticatedError();
            }
            return;
        }
        logger.info("heart succeeded: "  );
    }

    public Task getTasks() throws UnauthenticatedError {
        logger.info("Will try to get tasks");
        RequestContext request = RequestContext.newBuilder().setAuthToken(sessionToken).build();
        Task response;
        try {
            response = blockingStub.getTask(request);
            logger.info("getTasks succeeded. orders count: " +  response.getOrdersCount());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());

            if (e.getStatus() == Status.UNAUTHENTICATED) {
                throw new UnauthenticatedError();
            }

            return null;
        }

        return response;
    }

    public boolean completeTask(Task task) {
        task.getOrdersList().forEach(order -> {
            logger.info("Fulfilled order #" + order.getId());
        });


        return true;
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {
        // Access a service running on the local machine on port 50051
        io.taskscheduler.TaskWorker client = new io.taskscheduler.TaskWorker(HOST, PORT);

        WorkerStatus status = WorkerStatus.UNAUTHENTICATED;
        final int defaultSleep = 1000;
        int sleepTime = defaultSleep;

        while(true) {
            sleep(sleepTime);
            if (status == WorkerStatus.UNAUTHENTICATED) {
                try {
                    client.registerWorker();
                    status = WorkerStatus.READY;
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Server has come to a halt");
                    logger.log(Level.SEVERE, e.getMessage());
                    break;
                }
            } else if (status == WorkerStatus.READY) {
                try {
                    Task task = client.getTasks();
                    if (task == null) {
                        continue;
                    }

                    // TODO: implement error handling
                    client.completeTask(task);
                } catch (UnauthenticatedError e) {
                    status = WorkerStatus.UNAUTHENTICATED;
                    // prevent it from sleeping
                    sleepTime = 0;
                    continue;
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Server has come to a halt");
                    logger.log(Level.SEVERE, e.getMessage());
                    break;
                }
            }
            sleepTime = defaultSleep;
        }
        client.shutdown();
    }
}

/**
 * TODO: convert to its own file
 */
enum WorkerStatus {
    READY,
    UNAUTHENTICATED,
}

class UnauthenticatedError extends Exception {}