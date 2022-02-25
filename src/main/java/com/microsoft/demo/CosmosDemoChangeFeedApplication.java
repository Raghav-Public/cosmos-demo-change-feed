package com.microsoft.demo;

import java.time.Instant;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.microsoft.sample.api.helpers.ConfigurationHelper;
import com.microsoft.sample.cfp.Cfp;

@SpringBootApplication
public class CosmosDemoChangeFeedApplication {

	static ConfigurationHelper configHelper = new ConfigurationHelper();
	public static void main(String[] args) {
		System.out.print(args[0]);
		SpringApplication.run(CosmosDemoChangeFeedApplication.class, args);
		Instant startTime = Instant.parse("2022-02-24T19:00:00.00Z");
		Instant endTime = Instant.parse("2022-02-24T19:30:00.00Z");
		
		Cfp cfp = new Cfp(configHelper,startTime, endTime, "test001");
		if(cfp.init()) {
			cfp.start();
		}
		else {
			System.out.println("Cannot start the processor!");
		}
	}

}
