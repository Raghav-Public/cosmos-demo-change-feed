package com.microsoft.sample.api.helpers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;

import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class GenericHelper {
	
	private static String currentComputeIdentifier;
	public static String getCurrentComputeIdentifier() {
		if(currentComputeIdentifier == null) {
			currentComputeIdentifier = UUID.randomUUID().toString();
		}
		// This is used to identify the current compute unit. can be a predetermined value 
		return currentComputeIdentifier;
	}
	public static void logError(Throwable e, Logger logger) {
		logger.error(e.getMessage());
		logger.error(e.getStackTrace().toString());
	}
	public static void logDiagnostics(Logger logger, CosmosItemResponse<JsonNode> itemResponse) {
		logger.info("Activity Id: " + itemResponse.getActivityId());
		logger.info("Diagnostics: " + itemResponse.getDiagnostics().toString());
		logger.info("RU Charges: " + itemResponse.getRequestCharge());
		logger.info("Session Token: " + itemResponse.getSessionToken());
		logger.info("End-To-End Request Latency: " + itemResponse.getDuration());
	}
	public static void logDiagnostics(Logger logger, FeedResponse<JsonNode> feedResponse) {
		logger.info("Activity Id: " + feedResponse.getActivityId());
		logger.info("Diagnostics: " + feedResponse.getCosmosDiagnostics().toString());
		logger.info("RU Charges: " + feedResponse.getRequestCharge());
		logger.info("Session Token: " + feedResponse.getSessionToken());
	}
	public static void logDiagnostics(Logger logger, CosmosException exceptionResponse) {
		logger.info("Activity Id: " + exceptionResponse.getActivityId());
		logger.info("Diagnostics: " + exceptionResponse.getDiagnostics().toString());
		logger.info("Status Code: " + exceptionResponse.getStatusCode());
		logger.info("Sub Status Code: " + exceptionResponse.getSubStatusCode());
		logger.info("Retry After: " + exceptionResponse.getRetryAfterDuration());
	}
	public static JsonNode getErrorJson(Object error, int statusCode) {
		String strError = "";
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode node = null;
		try {
			if(error instanceof Throwable) {
				Throwable t = (Throwable)error;
				strError = StringEscapeUtils.escapeJava(t.getMessage());
			}
			else {
				Exception e = (Exception)error;
				strError = StringEscapeUtils.escapeJava(e.getMessage());
			}
			String errorJson = "{\"status\":" + statusCode + ", \"message\":\"" + strError + "\"}";
			node = objectMapper.readTree(errorJson);
	
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return node;
	}
	
	public static SqlQuerySpec getSqlQueryFromQueryString(String filters, Logger logger) {
		
		SqlQuerySpec sqlQuerySpec = new SqlQuerySpec();
		ObjectMapper mapper = new ObjectMapper();
		String queryText = "SELECT * FROM c";
		
		List<SqlParameter> sqlParameters = new ArrayList<SqlParameter>();
		try {
			JsonNode node = new ObjectMapper().readValue(filters, JsonNode.class);
			Map<String, Object> allFilters = mapper.convertValue(node, Map.class);
			if(allFilters.size() > 0) {
				queryText += " WHERE ";
				Iterator<Map.Entry<String, Object>> iterator = allFilters.entrySet().iterator();
				
				while(iterator.hasNext()) {
					Map.Entry<String, Object> entry = iterator.next();
					queryText += "c." + entry.getKey() + " = @" + entry.getKey();
					SqlParameter sqlParameter = new SqlParameter("@" + entry.getKey(), entry.getValue());
					sqlParameters.add(sqlParameter);
					if(iterator.hasNext()) {
						queryText += " AND ";
					}
				}
			}
			
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(Exception exp) {
			exp.printStackTrace();
		}
		logger.info(queryText);
		sqlQuerySpec.setQueryText(queryText);
		sqlQuerySpec.setParameters(sqlParameters);
		return sqlQuerySpec;
	}
	public static JsonNode handleException(Exception exp, Logger logger) {
		if(exp instanceof CosmosException) {
			CosmosException cosmosException = (CosmosException) exp;
			GenericHelper.logDiagnostics(logger, cosmosException);
			return GenericHelper.getErrorJson(cosmosException, cosmosException.getStatusCode());
		}
		logError(exp, logger);
		return getErrorJson(exp, 500);
	}
}
