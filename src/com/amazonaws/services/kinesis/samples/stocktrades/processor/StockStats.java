/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade.TradeType;

/**
 * Maintains running statistics of stock trades passed to it.
 *
 */
public class StockStats {

    // Keeps count of trades for each ticker symbol for each trade type
    private EnumMap<TradeType, Map<String, Long>> countsByTradeType;

    // Keeps the ticker symbol for the most popular stock for each trade type
    private EnumMap<TradeType, String> mostPopularByTradeType;

    // Ticker symbol of the stock that had the largest quantity of shares sold 
    private String largestSellOrderStock;

    // Quantity of shares for the largest sell order trade
    private long largestSellOrderQuantity;

    /**
     * Constructor.
     */
    public StockStats() {
        countsByTradeType = new EnumMap<TradeType, Map<String, Long>>(TradeType.class);
        for (TradeType tradeType: TradeType.values()) {
            countsByTradeType.put(tradeType, new HashMap<String, Long>());
        }

        mostPopularByTradeType = new EnumMap<TradeType, String>(TradeType.class);
    }

    /**
     * Updates the statistics taking into account the new stock trade received.
     *
     * @param trade Stock trade instance
     */
    public void addStockTrade(StockTrade trade) {
        // update buy/sell count
        TradeType type = trade.getTradeType();
        Map<String, Long> counts = countsByTradeType.get(type);
        Long count = counts.get(trade.getTickerSymbol());
        if (count == null) {
            count = 0L;
        }
        counts.put(trade.getTickerSymbol(), ++count);

        // update most popular stock
        String mostPopular = mostPopularByTradeType.get(type);
        if (mostPopular == null ||
                countsByTradeType.get(type).get(mostPopular) < count) {
            mostPopularByTradeType.put(type, trade.getTickerSymbol());
        }

        // update largest sell order
        if (type == TradeType.SELL) {
            if (largestSellOrderStock == null || trade.getQuantity() > largestSellOrderQuantity) {
                largestSellOrderStock = trade.getTickerSymbol();
                largestSellOrderQuantity = trade.getQuantity();
            }
        }
    }

    public String toString() {
        return String.format(
                "Most popular stock being bought: %s, %d buys.%n" +
                "Most popular stock being sold: %s, %d sells.%n" +
                "Largest sell order: %d shares of %s.",
                getMostPopularStock(TradeType.BUY), getMostPopularStockCount(TradeType.BUY),
                getMostPopularStock(TradeType.SELL), getMostPopularStockCount(TradeType.SELL),
                largestSellOrderQuantity, largestSellOrderStock);
    }

    private String getMostPopularStock(TradeType tradeType) {
        return mostPopularByTradeType.get(tradeType);
    }

    private Long getMostPopularStockCount(TradeType tradeType) {
        String mostPopular = getMostPopularStock(tradeType);
        return countsByTradeType.get(tradeType).get(mostPopular);
    }
}
