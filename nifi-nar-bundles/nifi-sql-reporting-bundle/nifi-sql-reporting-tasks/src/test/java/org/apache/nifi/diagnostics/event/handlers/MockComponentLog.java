package org.apache.nifi.diagnostics.event.handlers;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

public class MockComponentLog implements ComponentLog {

    String infoMessage;
    String warnMessage;
    String debugMessage;
    String traceMessage;
    String errorMessage;

    @Override
    public void warn(String msg, Throwable t) {
        warn(msg);
    }

    @Override
    public void warn(String msg, Object[] os) {
        warn(msg);
    }

    @Override
    public void warn(String msg, Object[] os, Throwable t) {
        warn(msg);
    }

    @Override
    public void warn(String msg) {
        warnMessage = msg;
    }

    @Override
    public void trace(String msg, Throwable t) {
        trace(msg);
    }

    @Override
    public void trace(String msg, Object[] os) {
        trace(msg);
    }

    @Override
    public void trace(String msg) {
        traceMessage = msg;
    }

    @Override
    public void trace(String msg, Object[] os, Throwable t) {
        trace(msg);
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void info(String msg, Throwable t) {
        info(msg);
    }

    @Override
    public void info(String msg, Object[] os) {
        info(msg);
    }

    @Override
    public void info(String msg) {
        infoMessage = msg;
    }

    @Override
    public void info(String msg, Object[] os, Throwable t) {
        info(msg);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void error(String msg, Throwable t) {
        error(msg);
    }

    @Override
    public void error(String msg, Object[] os) {
        error(msg);
    }

    @Override
    public void error(String msg) {
        errorMessage = msg;
    }

    @Override
    public void error(String msg, Object[] os, Throwable t) {
        error(msg);
    }

    @Override
    public void debug(String msg, Throwable t) {
        debug(msg);
    }

    @Override
    public void debug(String msg, Object[] os) {
        debug(msg);
    }

    @Override
    public void debug(String msg, Object[] os, Throwable t) {
        debug(msg);
    }

    @Override
    public void debug(String msg) {
        debugMessage = msg;
    }

    @Override
    public void log(LogLevel level, String msg, Throwable t) {

    }

    @Override
    public void log(LogLevel level, String msg, Object[] os) {

    }

    @Override
    public void log(LogLevel level, String msg) {
        switch (level) {
            case WARN:
                warn(msg);
                break;
            case DEBUG:
                debug(msg);
                break;
            case INFO:
                info(msg);
                break;
            case ERROR:
                error(msg);
                break;
            case TRACE:
                trace(msg);
                break;
            case FATAL:
                error(msg);
                break;
            case NONE:
                info(msg);
                break;
        }
    }

    @Override
    public void log(LogLevel level, String msg, Object[] os, Throwable t) {

    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public String getWarnMessage() {
        return warnMessage;
    }

    public String getDebugMessage() {
        return debugMessage;
    }

    public String getTraceMessage() {
        return traceMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
