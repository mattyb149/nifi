/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.reporting.sql.connectionstatuspredictions;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingContext;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class ConnectionStatusPredictionsEnumerator implements Enumerator<Object> {
    private final ReportingContext context;
    private final ComponentLog logger;
    private final int[] fields;

    private Deque<Iterator<ProcessGroupStatus>> iteratorBreadcrumb;
    private Iterator<ConnectionStatus> connectionStatusIterator;
    private Object currentRow;
    private int recordsRead = 0;

    public ConnectionStatusPredictionsEnumerator(final ReportingContext context, final ComponentLog logger, final int[] fields) {
        this.context = context;
        this.logger = logger;
        this.fields = fields;
        reset();
    }

    @Override
    public Object current() {
        return currentRow;
    }

    @Override
    public boolean moveNext() {
        currentRow = null;
        if (iteratorBreadcrumb.isEmpty()) {
            // Start the breadcrumb trail to follow recursively into process groups looking for connections
            ProcessGroupStatus rootStatus = context.getEventAccess().getControllerStatus();
            iteratorBreadcrumb.push(rootStatus.getProcessGroupStatus().iterator());
            connectionStatusIterator = rootStatus.getConnectionStatus().iterator();
        }

        final ConnectionStatus connectionStatus = getNextConnectionStatusPrediction();
        if (connectionStatus == null) {
            // If we are out of data, close the InputStream. We do this because
            // Calcite does not necessarily call our close() method.
            close();
            try {
                onFinish();
            } catch (final Exception e) {
                logger.error("Failed to perform tasks when enumerator was finished", e);
            }

            return false;
        }

        currentRow = filterColumns(connectionStatus);

        recordsRead++;
        return true;
    }

    protected int getRecordsRead() {
        return recordsRead;
    }

    protected void onFinish() {
    }

    private Object filterColumns(final ConnectionStatus status) {
        if (status == null) {
            return null;
        }

        final ConnectionStatusPredictions predictions = status.getPredictions();

        final Object[] row = new Object[]{
                status.getId(),
                predictions.getNextPredictedQueuedBytes(),
                predictions.getNextPredictedQueuedCount(),
                predictions.getPredictedPercentBytes(),
                predictions.getPredictedPercentCount(),
                predictions.getPredictedTimeToBytesBackpressureMillis(),
                predictions.getPredictedTimeToCountBackpressureMillis(),
                predictions.getPredictionIntervalMillis()
        };

        // If we want no fields just return null
        if (fields == null) {
            return row;
        }

        // If we want only a single field, then Calcite is going to expect us to return
        // the actual value, NOT a 1-element array of values.
        if (fields.length == 1) {
            final int desiredCellIndex = fields[0];
            return row[desiredCellIndex];
        }

        // Create a new Object array that contains only the desired fields.
        final Object[] filtered = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final int indexToKeep = fields[i];
            filtered[i] = row[indexToKeep];
        }

        return filtered;
    }

    @Override
    public void reset() {
        // Clear the root PG status object so it is fetched fresh on the first record
        connectionStatusIterator = null;
        iteratorBreadcrumb = new LinkedList<>();
    }

    @Override
    public void close() {
    }

    private ConnectionStatus getNextConnectionStatusPrediction() {
        if (connectionStatusIterator != null && connectionStatusIterator.hasNext()) {
            return connectionStatusIterator.next();
        }
        // No more connections in this PG, so move to the next
        connectionStatusIterator = null;
        Iterator<ProcessGroupStatus> i = iteratorBreadcrumb.peek();
        if (i == null) {
            return null;
        }

        if (i.hasNext()) {
            ProcessGroupStatus nextPG = i.next();
            iteratorBreadcrumb.push(nextPG.getProcessGroupStatus().iterator());
            connectionStatusIterator = nextPG.getConnectionStatus().iterator();
            return getNextConnectionStatusPrediction();
        } else {
            // No more child PGs, remove it from the breadcrumb trail and try again
            iteratorBreadcrumb.pop();
            return getNextConnectionStatusPrediction();
        }
    }
}
