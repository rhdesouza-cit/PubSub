package com.poc.pubsub;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PubsubApplication {

    private static final Log LOGGER = LogFactory.getLog(PubsubApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(PubsubApplication.class, args);
    }

}
