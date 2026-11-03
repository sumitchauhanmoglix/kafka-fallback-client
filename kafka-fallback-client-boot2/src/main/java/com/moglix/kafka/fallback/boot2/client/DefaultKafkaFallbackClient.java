package com.moglix.kafka.fallback.boot2.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.client.AbstractKafkaFallbackClient;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import com.moglix.kafka.fallback.resilience.CircuitBreaker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Spring Boot 2 implementation of KafkaFallbackOperations.
 * Adapts Spring Kafka 2.8's ListenableFuture into CompletableFuture.
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
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (kafkaTemplate == null) {
            future.completeExceptionally(new IllegalStateException("KafkaTemplate is not configured in Spring application context"));
            return future;
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

            ListenableFuture<SendResult<Object, Object>> listenableFuture = kafkaTemplate.send(record);
            listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {
                @Override
                public void onSuccess(SendResult<Object, Object> result) {
                    future.complete(null);
                }

                @Override
                public void onFailure(Throwable ex) {
                    future.completeExceptionally(ex);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        return future;
    }

    public KafkaTemplate<Object, Object> getKafkaTemplate() {
        return kafkaTemplate;
    }
}
