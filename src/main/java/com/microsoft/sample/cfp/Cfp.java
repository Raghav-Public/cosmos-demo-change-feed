package com.microsoft.sample.cfp;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.sample.api.dal.CosmosAsyncDAL;
import com.microsoft.sample.api.helpers.ConfigurationHelper;

public class Cfp {
	
	private String hostName;
	private ConfigurationHelper helper;
	private Instant startTime;
	private Instant endTime;
	private CosmosAsyncContainer primaryContainer;
	private CosmosAsyncContainer leaseContainer;
	private ChangeFeedProcessor changeFeedProcessor;
	private int runCount;
	
	public Cfp(ConfigurationHelper helper, Instant sTime, Instant eTime, String hostName) {
		this.startTime = sTime;
		this.endTime = eTime;
		this.helper = helper;
		this.hostName = hostName;
	}
	
	public Boolean init() {
		try {
			CosmosAsyncDAL cosmosAsyncDALPrimary = CosmosAsyncDAL.getInstance(helper.getProperty("host"), 
																				helper.getProperty("key"),
																				helper.getProperty("db"),
																				helper.getProperty("col"));
			primaryContainer = cosmosAsyncDALPrimary.getAsyncContainer();
			
			CosmosAsyncDAL cosmosAsyncDALLease = CosmosAsyncDAL.getInstance(helper.getProperty("lease_host"), 
																			helper.getProperty("lease_key"),
																			helper.getProperty("lease_db"),
																			helper.getProperty("lease_col"));
			leaseContainer = cosmosAsyncDALLease.getAsyncContainer();
			initChangeFeedProcessor();
		}
		catch(Exception exp) {
			System.out.println(exp.getMessage());
			return false;
		}
		return true;
	}
	
	public void start() {
		runCount = 0;
		this.changeFeedProcessor.start().subscribe();
	}
	public void stop() {
		this.changeFeedProcessor.stop().subscribe();
	}
	
	private void initChangeFeedProcessor() {	
		ChangeFeedProcessorOptions cfpOptions = new ChangeFeedProcessorOptions();
		cfpOptions.setFeedPollDelay(Duration.ofMillis(100));
		cfpOptions.setStartTime(startTime);
		
		changeFeedProcessor = new ChangeFeedProcessorBuilder()
										.options(cfpOptions)
										.hostName(hostName)
										.feedContainer(primaryContainer)
										.leaseContainer(leaseContainer)
										.handleChanges((List<JsonNode> docs) -> {
								            for (JsonNode document : docs) {
								            	changeFeedHandler(document);
								            }

								        })
								        .buildChangeFeedProcessor();
	}
	
	private void changeFeedHandler(JsonNode document) {
		long _ts = document.get("_ts").asLong();
		System.out.println(runCount);
		runCount++;
		
		if(Instant.ofEpochSecond(_ts).isAfter(endTime)) {
			System.out.println(_ts + "<--------------->" + endTime.getEpochSecond());
			stop();
		}
		System.out.println(document.toPrettyString());
	}

}