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

package com.amazonaws.services.kinesis.samples.stocktrades.writer;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.samples.stocktrades.model.StockTrade;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.CredentialUtils;

/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class StockTradesWriter {

    private static final Log LOG = LogFactory.getLog(StockTradesWriter.class);
    private static boolean table_exist = false;

    private static void checkUsage(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: " + StockTradesWriter.class.getSimpleName()
                    + " <stream name> <region>");
            System.exit(1);
        }
    }

/**
	 * Checks if the stream exists and is active and if not creates another stream
	 * Make sure the user has access for describe and List streams on kinesis
	 *
	 * @param shardCount
	 * 
	 * @param kinesisClient
	 *            Amazon Kinesis client instance
	 * @param streamName
	 *            Name of stream
	 * @throws InterruptedException
	 */
	private static void validateStream(AmazonKinesis kinesisClient, String streamName) throws InterruptedException {

		String str = streamName;
		com.amazonaws.services.kinesis.model.ListStreamsResult list_stream_obj = kinesisClient.listStreams();

		
		ArrayList<String> stream_names_list = (ArrayList<String>) list_stream_obj.getStreamNames();
		Iterator<String> itr = stream_names_list.iterator();
		while (itr.hasNext()) {
			String streams = (String) itr.next();

			if (streams.equals(str)) {
				System.out.println("The stream specified exists and we will push sample data..");
				table_exist = true;
			}

		}

		if (table_exist == false) {
			LOG.info("The stream specified does not exists..we will be creating it..");
			CreateStreamRequest createRequest = new CreateStreamRequest();
			createRequest.setShardCount(1);
			createRequest.setStreamName(streamName);
			kinesisClient.createStream(createRequest);
		}

		DescribeStreamResult result = kinesisClient.describeStream(streamName);
		String tabl_stat = result.getStreamDescription().getStreamStatus();
		//LOG.info("Table status is..." + tabl_stat);

		while ((result.getStreamDescription().getStreamStatus()).equals("UPDATING")
				|| (result.getStreamDescription().getStreamStatus()).equals("CREATING")) {
			Thread.sleep(10000);
			LOG.info("Sleeping as shard not active yet..");

			result = kinesisClient.describeStream(streamName);
			tabl_stat = result.getStreamDescription().getStreamStatus();

		}
		LOG.info("Table status is..." + tabl_stat);

	}


    /**
     * Uses the Kinesis client to send the stock trade to the given stream.
     *
     * @param trade instance representing the stock trade
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */
    private static void sendStockTrade(StockTrade trade, AmazonKinesis kinesisClient,
            String streamName) {
        byte[] bytes = trade.toJsonAsBytes();
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade");
            return;
        }

        LOG.info("Putting trade: " + trade.toString());
        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        // We use the ticker symbol as the partition key, as explained in the tutorial.
        putRecord.setPartitionKey(trade.getTickerSymbol());
        putRecord.setData(ByteBuffer.wrap(bytes));

        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }

    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String streamName = args[0];
        String regionName = args[1];
        Region region = RegionUtils.getRegion(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        AWSCredentials credentials = CredentialUtils.getCredentialsProvider().getCredentials();

        AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials,
                ConfigurationUtils.getClientConfigWithUserAgent());
        kinesisClient.setRegion(region);

        // Validate that the stream exists and is active
        validateStream(kinesisClient, streamName);

        // Repeatedly send stock trades with a 100 milliseconds wait in between
        StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();
        while(true) {
            StockTrade trade = stockTradeGenerator.getRandomTrade();
            sendStockTrade(trade, kinesisClient, streamName);
            Thread.sleep(100);
        }
    }

}
