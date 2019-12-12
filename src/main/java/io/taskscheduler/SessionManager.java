package io.taskscheduler;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.time.LocalDateTime;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SessionManager {
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<String, Session>();
    private final int maxSessions;

    public SessionManager(int maxSessions) {
        this.maxSessions = maxSessions;
    }

    public Session createSession() {
        String id = generateUniqueSessionId();
        Session session = new Session(id);
        sessions.put(id, session);

        return session;
    }

    public Session retrieveSession(String id) {
        Session session = sessions.getOrDefault(id, null);
        if (session == null) {
            return null;
        }

        if(hasSessionExpired(session)) {
            return null;
        }

        return session;
    }

    private boolean hasSessionExpired(Session session) {
        // TODO: implement this
        return false;
    }

    private void removeSession(Session session) {
        sessions.remove(session.id);
    }

    private String generateUniqueSessionId() {
        // generate unused sessionId
        String id = null;
        do {
            id = UUID.randomUUID().toString();
        } while (sessions.containsKey(id));
        return id;
    }
}

class Session {
    public String id;
    public LocalDateTime lastActivity;

    public TimeSeriesData tasksReceived;
    public TimeSeriesData tasksCompleted;
    public TimeSeriesData tasksFailed;

    public Session (String id) {
        this.id = id;
        this.lastActivity = LocalDateTime.now();
        this.tasksReceived = new TimeSeriesData(20);
        this.tasksFailed = new TimeSeriesData(20);
        this.tasksCompleted = new TimeSeriesData(20);
    }

    public void refreshSession() {
        this.lastActivity = LocalDateTime.now();
    }

    public void addStats(LocalDateTime time, long tasksReceived, long tasksCompleted, long tasksFailed) {
        this.tasksReceived.addDataPoint(time, tasksReceived);
        this.tasksCompleted.addDataPoint(time, tasksCompleted);
        this.tasksFailed.addDataPoint(time, tasksFailed);
    }
}

class TimeSeriesData {
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    TimeSeriesData(int dataLifetimeSec) {

    }

    public void addDataPoint(LocalDateTime time, long amount) {
        writeLock.lock();
        try {
            // add data
        } finally {
            writeLock.unlock();
        }
    }
}