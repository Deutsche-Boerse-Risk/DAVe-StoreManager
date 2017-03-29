package com.deutscheboerse.risk.dave.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private final String className;
    private final Map<Level, ILoggingEvent> lastLogMessage = new ConcurrentHashMap<>();
    private final Map<Level, Integer> messageCount = new ConcurrentHashMap<>();

    private TestAppender(String className) {
        this.className = className;
        this.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
    }

    public static TestAppender getAppender(final Class<?> clazz) {
        return new TestAppender(clazz.getName());
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (event.getLoggerName().equals(this.className)) {
            synchronized(this) {
                lastLogMessage.put(event.getLevel(), event);
                this.messageCount.compute(event.getLevel(), (k, v) -> (v == null) ? 1 : v + 1);
                this.notifyAll();
            }
        }
    }

    public ILoggingEvent getLastMessage(Level level) throws InterruptedException {
        synchronized(this) {
            while (!this.lastLogMessage.containsKey(level)) {
                this.wait(5000);
            }
        }
        return lastLogMessage.get(level);
    }

    public void waitForMessageCount(Level level, int count) throws InterruptedException {
        synchronized(this) {
            while (this.messageCount.getOrDefault(level, 0) < count) {
                this.wait(5000);
            }
        }
    }

    public void waitForMessageContains(Level level, String message) throws InterruptedException {
        synchronized(this) {
            while (!this.getLastMessage(level).getFormattedMessage().contains(message)) {
                this.wait(5000);
            }
        }
    }

    @Override
    public void stop() {
        lastLogMessage.clear();
        super.stop();
    }
}