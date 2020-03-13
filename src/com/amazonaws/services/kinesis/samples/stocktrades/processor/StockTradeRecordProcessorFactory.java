/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Used to create new stock trade record processors.
 *
 */
public class StockTradeRecordProcessorFactory implements IRecordProcessorFactory {

    /**
     * Constructor.
     */
    public StockTradeRecordProcessorFactory() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new StockTradeRecordProcessor();
    }

}
