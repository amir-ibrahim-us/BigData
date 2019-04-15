package com.amir.Kinesis_producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication
public class KinesisProducerApplication extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application){
		return application.sources(KinesisProducerApplication.class);
	}
	public static void main(String[] args) {
		SpringApplication.run(KinesisProducerApplication.class, args);
	}

}
