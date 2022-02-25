package com.microsoft.sample.api.helpers;

import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;

import ch.qos.logback.core.util.Duration;

public class ConnectionHelper {
	
	public static DirectConnectionConfig getDirectModeConfig() {
		
		DirectConnectionConfig config = DirectConnectionConfig.getDefaultConfig();
		
		
		/* 
		 * Sets the connect timeout for direct client,
		 * represents timeout for establishing connections with an endpoint. 
		 * Configures timeout for underlying Netty Channel Channel Option.CONNECT_TIMEOUT_MILLIS 
		 * By default, the connect timeout is 5 seconds.
		 * millis =  current millisecond instant of the clock
		 */
		
		//sample uses default
		//config.setConnectTimeout(new Duration(millis));
		return config;
	}
	
	public static GatewayConnectionConfig getGatewayConfig() {
		GatewayConnectionConfig config = GatewayConnectionConfig.getDefaultConfig();
		return config;
	}

}
