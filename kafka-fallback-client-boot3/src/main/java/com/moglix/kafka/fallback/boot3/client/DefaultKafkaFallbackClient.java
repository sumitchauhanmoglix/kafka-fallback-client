package com.moglix.kafka.fallback.boot3.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.client.AbstractKafkaFallbackClient;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import com.moglix.kafka.fallback.resilience.CircuitBreaker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Spring Boot 3 implementation of KafkaFallbackOperations.
 * Utilizes Spring Kafka 3.x native CompletableFuture.
 */
public class DefaultKafkaFallbackClient extends AbstractKafkaFallbackClient {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public DefaultKafkaFallbackClient(KafkaFallbackProperties properties,
                                      String defaultApplicationName,
                                      ObjectMapper objectMapper,
                                      CircuitBreaker circuitBreaker,
                                      FallbackHttpClient fallbackHttpClient,
                                      KafkaTemplate<Object, Object> kafkaTemplate) {
        super(properties, defaultApplicationName, objectMapper, circuitBreaker, fallbackHttpClient);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    protected CompletableFuture<Void> doSendToKafka(String topic, String payload, Map<String, String> headers) {
        if (kafkaTemplate == null) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("KafkaTemplate is not configured in Spring application context"));
            return failed;
        }

        try {
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, payload);
            if (headers != null && !headers.isEmpty()) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    if (entry.getKey() != null) {
                        byte[] headerValue = entry.getValue() != null ?
                                entry.getValue().getBytes(StandardCharsets.UTF_8) : new byte[0];
                        record.headers().add(new RecordHeader(entry.getKey(), headerValue));
                    }
                }
            }

            return kafkaTemplate.send(record).thenApply(result -> null);
        } catch (Throwable t) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }
    }

    public KafkaTemplate<Object, Object> getKafkaTemplate() {
        return kafkaTemplate;
    }
}
