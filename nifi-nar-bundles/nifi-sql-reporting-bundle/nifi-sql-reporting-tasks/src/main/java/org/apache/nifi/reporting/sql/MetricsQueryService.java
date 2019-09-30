package org.apache.nifi.reporting.sql;

import com.github.benmanes.caffeine.cache.Cache;

import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.util.concurrent.BlockingQueue;

public interface MetricsQueryService {
    QueryResult query(final ReportingContext context, final String sql) throws Exception;
    ResultSetRecordSet getResultSetRecordSet(QueryResult queryResult) throws Exception;
    void closeQuietly(final AutoCloseable... closeables);
}
