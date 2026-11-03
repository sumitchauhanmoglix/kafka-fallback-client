package com.moglix.kafka.fallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaFallbackClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaFallbackClient.class);

    public KafkaFallbackClient() {
        log.info("KafkaFallbackClient initialized for Spring Boot 2");
    }
}
