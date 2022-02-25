package com.microsoft.sample.api.dal;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.ConflictException;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.microsoft.sample.api.helpers.ConfigurationHelper;
import com.microsoft.sample.api.helpers.ConnectionHelper;
import com.microsoft.sample.api.helpers.GenericHelper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CosmosAsyncDAL {
	private String host;
	private String key;
	private String databaseName;
	private String containerName;
	private CosmosAsyncClient client;
	private CosmosAsyncDatabase database;
	private CosmosAsyncContainer container;
	
	private static Logger LOGGER = LoggerFactory.getLogger(CosmosAsyncDAL.class);
	// Singleton 
	private static CosmosAsyncDAL cosmosDAL = null;
	
	public static CosmosAsyncDAL getInstance(String host, String key, String databaseName, String containerName) {
		//if(cosmosDAL == null) {
			cosmosDAL = new CosmosAsyncDAL(host, key, databaseName, containerName);
			cosmosDAL.init();
		//}
		return cosmosDAL;
	}
	
	
	private CosmosAsyncDAL(String host, String key, String dbName, String cName) {
		this.host = host;
		this.key = key;
		this.databaseName = dbName;
		this.containerName = cName;
	}
	
	public static CosmosAsyncDAL getInstance(ConfigurationHelper configHelper) {
		CosmosAsyncDAL cosmosAsyncDal = null;
		try {
			cosmosAsyncDal = CosmosAsyncDAL.getInstance(configHelper.getProperty("host"),
											  configHelper.getProperty("key"),
											  configHelper.getProperty("db"),
											  configHelper.getProperty("col"));
		}
		catch(Exception exp) {
			System.out.println(exp.getMessage());
		}
		return cosmosAsyncDal;
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
	        		.contentResponseOnWriteEnabled(true)
	        		.buildAsyncClient();
			LOGGER.info("Async cosmos client initiated");
			
			// assuming database was created using portal or cli
			database = client.getDatabase(databaseName);
			LOGGER.info("Obtained Database Context: " + databaseName);
			// assuming container was created using portal or cli
			container = database.getContainer(containerName);
			LOGGER.info("Obtained Container Context: " + containerName);
			
			//Mono<CosmosContainerResponse> containerResponseMono = container.read();
			//CosmosContainerResponse containerResponse = containerResponseMono.block();
			//long progress = containerResponse.getIndexTransformationProgress();
		}
		catch(Exception exp) {
			LOGGER.error(exp.getMessage());
			return false;
		}
		return true;
	}
	
	public CosmosAsyncContainer getAsyncContainer() {
		return this.container;
	}
	
	public Mono<JsonNode> create(JsonNode data) {
		Mono<CosmosItemResponse<JsonNode>> itemResponse = container.createItem(data);
		return itemResponse.flatMap(ir -> {
			GenericHelper.logDiagnostics(LOGGER, ir);
			return Mono.just(ir.getItem());
		}).onErrorResume(e -> {	
			GenericHelper.logError(e, LOGGER);
			return Mono.just(GenericHelper.getErrorJson(e, 400));
		});
	}
	
	/*
	 * Point Read
	 */
	public JsonNode retrieve(String id, String pk) {
		
		LOGGER.info("id:" + id + " pk:" + pk);
		Mono<CosmosItemResponse<JsonNode>> itemResponseMono = container.readItem(id, new PartitionKey(pk), JsonNode.class);
		
		CosmosItemResponse<JsonNode> itemResponse = itemResponseMono.block();
		CosmosDiagnostics diagnostics = itemResponse.getDiagnostics();
		
		LOGGER.info(diagnostics.toString());
		
		return itemResponse.getItem();
	}
	
	/*
	 * Query option
	 */
	public Flux<List<JsonNode>> query(String filters) {
		
		CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
		
		queryOptions.setQueryMetricsEnabled(true);
		try {
			SqlQuerySpec sqlQuerySpec = GenericHelper.getSqlQueryFromQueryString(filters, LOGGER);
			//LOGGER.info(sqlQuerySpec.getQueryText());
			CosmosPagedFlux<JsonNode> itemPages = container.queryItems(sqlQuerySpec, queryOptions, JsonNode.class);
			
			return itemPages.byPage().flatMap(ip-> {
				LOGGER.info(ip.getCosmosDiagnostics().toString());
				return Flux.just(ip.getResults());
			});
		}
		catch(Exception exp) {
			System.out.println(exp.getMessage());
		}
		return null;
	}
	
	public Mono<JsonNode> update(JsonNode data) {
		Mono<CosmosItemResponse<JsonNode>> itemResponse = container.upsertItem(data);
		return itemResponse.flatMap(ir -> {
			GenericHelper.logDiagnostics(LOGGER, ir);
			return Mono.just(ir.getItem());
		}).onErrorResume(e -> {	
			GenericHelper.logError(e, LOGGER);
			return Mono.just(GenericHelper.getErrorJson(e, 400));
		});
	}
	
	public Mono<Object> delete(String partitionKey, String itemId) {
		System.out.println(partitionKey);
		Mono<CosmosItemResponse<Object>> itemResponse = container.deleteItem(itemId, new PartitionKey(partitionKey));
		return itemResponse.flatMap(ir -> {
			//GenericHelper.logDiagnosticsRUcharges(LOGGER, ir);
			return Mono.empty();
		});
	}
	
}
