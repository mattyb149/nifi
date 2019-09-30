package org.apache.nifi.reporting.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;

class CachedStatement {
    private final PreparedStatement statement;
    private final Connection connection;

    CachedStatement(final PreparedStatement statement, final Connection connection) {
        this.statement = statement;
        this.connection = connection;
    }

    PreparedStatement getStatement() {
        return statement;
    }

    Connection getConnection() {
        return connection;
    }
}
