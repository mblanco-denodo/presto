/*
 * Copyright (c) 2025. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 */
package com.facebook.presto.plugin.denodo.arrow.split;

import com.facebook.plugin.arrow.ArrowSplitManager;
import com.facebook.plugin.arrow.ArrowTableHandle;
import com.facebook.plugin.arrow.ArrowTableLayoutHandle;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.presto.plugin.denodo.arrow.DenodoArrowFlightClientHandler;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import jakarta.inject.Inject;
import org.apache.arrow.flight.FlightInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DenodoArrowSplitManager
        extends ArrowSplitManager
{
    private static final Logger log = LoggerFactory.getLogger(DenodoArrowSplitManager.class);
    private final DenodoArrowFlightClientHandler clientHandler;
    private final Unsigned16BitCounter counter;

    @Inject
    public DenodoArrowSplitManager(BaseArrowFlightClientHandler clientHandler)
    {
        super(clientHandler);
        this.counter = new Unsigned16BitCounter();
        this.clientHandler = (DenodoArrowFlightClientHandler) requireNonNull(clientHandler, "clientHandler is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        ArrowTableLayoutHandle tableLayoutHandle = (ArrowTableLayoutHandle) layout;
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();
        // compute execution identifier
        String executionIdentifier = Integer.toString(this.counter.getValue());
        this.counter.increment();
        FlightInfo flightInfo = clientHandler.getFlightInfoForTableScan(session, tableLayoutHandle, executionIdentifier);
        log.info("sending split request with query + eId: {}", session.getQueryId() + '-' + executionIdentifier);
        List<DenodoArrowSplit> splits = flightInfo.getEndpoints()
                .stream()
                .map(info -> new DenodoArrowSplit(
                    tableHandle.getSchema(),
                    tableHandle.getTable(),
                    info.serialize().array(),
                    // the execution identifier is passed down on the split so that it can be communicated to the worker
                    executionIdentifier))
                .collect(toImmutableList());
        return new FixedSplitSource(splits);
    }
}
