package org.apache.nifi.reporting.sql;

import java.io.Closeable;
import java.sql.ResultSet;

interface QueryResult extends Closeable {
    ResultSet getResultSet();
    int getRecordsRead();
}
