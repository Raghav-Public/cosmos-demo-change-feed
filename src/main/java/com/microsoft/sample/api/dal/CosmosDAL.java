package com.microsoft.sample.api.dal;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.sample.api.helpers.ConfigurationHelper;
import com.microsoft.sample.api.helpers.ConnectionHelper;
import com.microsoft.sample.api.helpers.GenericHelper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CosmosDAL {
	private String host;
	private String key;
	private String databaseName;
	private String containerName;
	private CosmosClient client;
	private CosmosDatabase database;
	private CosmosContainer container;
	
	private static Logger LOGGER = LoggerFactory.getLogger(CosmosDAL.class);
	// Singleton 
	private static CosmosDAL cosmosDAL = null;
	
	public static CosmosDAL getInstance(String host, String key, String databaseName, String containerName) {
		if(cosmosDAL == null) {
			cosmosDAL = new CosmosDAL(host, key, databaseName, containerName);
			cosmosDAL.init();
		}
		return cosmosDAL;
	}
	
	public static CosmosDAL getInstance(ConfigurationHelper configHelper) {
		CosmosDAL cosmosDal = null;
		try {
			cosmosDal = CosmosDAL.getInstance(configHelper.getProperty("host"),
											  configHelper.getProperty("key"),
											  configHelper.getProperty("db"),
											  configHelper.getProperty("col"));
		}
		catch(Exception exp) {
			System.out.println(exp.getMessage());
		}
		return cosmosDal;
	}
	
	private CosmosDAL(String host, String key, String dbName, String cName) {
		this.host = host;
		this.key = key;
		this.databaseName = dbName;
		this.containerName = cName;
	}
	
	private Boolean init() {
		try {
			/* Direct Mode Connection Mode Client
			 * Will also need Gateway Mode config
			 * Session consistency is used, adjust the same accordingly
			 * TODO: add all the different initialization options
			 */
			LOGGER.info("Compute ID:" + GenericHelper.getCurrentComputeIdentifier());
			client = new CosmosClientBuilder()
	        		.endpoint(host)
	        		.key(key)
	        		.directMode(ConnectionHelper.getDirectModeConfig())
	        		//.gatewayMode(ConnectionHelper.getGatewayConfig())
	        		.consistencyLevel(ConsistencyLevel.SESSION)
	        		.userAgentSuffix(GenericHelper.getCurrentComputeIdentifier())
	        		.buildClient();
			LOGGER.info("Cosmos client initiated");
			
			// assuming database was created using portal or cli
			database = client.getDatabase(databaseName);
			
			// assuming container was created using portal or cli
			container = database.getContainer(containerName);
		}
		catch(Exception exp) {
			LOGGER.error(exp.getMessage());
			return false;
		}
		return true;
	}
	
	public JsonNode create(JsonNode data) {
		try {
			CosmosItemResponse<JsonNode> itemResponse = container.createItem(data);
			GenericHelper.logDiagnostics(LOGGER, itemResponse);
			return itemResponse.getItem();
		}
		catch(Exception exp) {
			return GenericHelper.handleException(exp, LOGGER);
		}
	}
	
	/*
	 * Point Read
	 */
	public JsonNode retrieve(String id, String pk) {
		try {
			CosmosItemResponse<JsonNode> itemResponse = container.readItem(id, new PartitionKey(pk), JsonNode.class);
			GenericHelper.logDiagnostics(LOGGER, itemResponse);
			//LOGGER.info(itemResponse.getC);
			return itemResponse.getItem();
		}
		catch(Exception exp) {
			return GenericHelper.handleException(exp, LOGGER);
		}
	}
	
	/*
	 * Query option
	 */
	public Flux<List<JsonNode>> query(String filters) {
		
		CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
		queryOptions.setQueryMetricsEnabled(true);
		
		try {
			SqlQuerySpec sqlQuerySpec = GenericHelper.getSqlQueryFromQueryString(filters, LOGGER);
			LOGGER.info(sqlQuerySpec.getQueryText());
			CosmosPagedIterable<JsonNode> itemPages = container.queryItems(sqlQuerySpec, queryOptions, JsonNode.class);
			//itemPages.iterableByPage(100);
		}
		catch(Exception exp) {
			System.out.println(exp.getMessage());
		}
		return null;
	}
	
	public JsonNode update(JsonNode data, String etag) {
		try {
			CosmosItemRequestOptions options = new CosmosItemRequestOptions();
			options.setIfMatchETag(etag);
			CosmosItemResponse<JsonNode> itemResponse = container.upsertItem(data, options);
			return itemResponse.getItem();
		}
		catch(Exception exp) {
			return GenericHelper.handleException(exp, LOGGER);
		}
	}
	
	public Object delete(String partitionKey, String itemId) {
		try {
			CosmosItemRequestOptions options = new CosmosItemRequestOptions();
			CosmosItemResponse<Object> itemResponse = container.deleteItem(itemId, new PartitionKey(partitionKey), options);
			return itemResponse.getItem();
		}
		catch(Exception exp) {
			return (Object) GenericHelper.handleException(exp, LOGGER);
		}
	}
}
