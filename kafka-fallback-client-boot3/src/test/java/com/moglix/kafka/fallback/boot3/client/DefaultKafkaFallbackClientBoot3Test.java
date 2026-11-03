package com.moglix.kafka.fallback.boot3.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moglix.kafka.fallback.http.FallbackHttpClient;
import com.moglix.kafka.fallback.model.PublishResult;
import com.moglix.kafka.fallback.enums.PublishStatus;
import com.moglix.kafka.fallback.properties.KafkaFallbackProperties;
import com.moglix.kafka.fallback.resilience.SimpleCircuitBreaker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultKafkaFallbackClientBoot3Test {

    private KafkaTemplate<Object, Object> mockKafkaTemplate;
    private FallbackHttpClient mockFallbackHttpClient;
    private DefaultKafkaFallbackClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockKafkaTemplate = mock(KafkaTemplate.class);
        mockFallbackHttpClient = mock(FallbackHttpClient.class);

        KafkaFallbackProperties props = new KafkaFallbackProperties();
        props.setApplicationName("boot3-test-app");
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(3, 5000);

        client = new DefaultKafkaFallbackClient(
                props,
                "boot3-test-app",
                new ObjectMapper(),
                cb,
                mockFallbackHttpClient,
                mockKafkaTemplate
        );
    }

    @Test
    @DisplayName("Should send ProducerRecord to Spring Kafka 3 KafkaTemplate with custom headers")
    @SuppressWarnings("unchecked")
    void testSendToKafkaWithHeaders() throws Exception {
        CompletableFuture<SendResult<Object, Object>> future = new CompletableFuture<>();
        ProducerRecord<Object, Object> dummyRecord = new ProducerRecord<>("orders", "dummy");
        RecordMetadata meta = new RecordMetadata(new TopicPartition("orders", 0), 0, 0, 0, 0L, 0, 0);
        future.complete(new SendResult<>(dummyRecord, meta));

        when(mockKafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        Map<String, String> headers = Collections.singletonMap("traceId", "boot3-trace-789");
        PublishResult result = client.publish("orders", "{\"orderId\":\"101\"}", headers);

        assertEquals(PublishStatus.SENT_TO_KAFKA, result.getStatus());
        assertTrue(result.isSentToKafka());

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockKafkaTemplate).send(captor.capture());

        ProducerRecord<Object, Object> captured = captor.getValue();
        assertEquals("orders", captured.topic());
        assertEquals("{\"orderId\":\"101\"}", captured.value());
        assertNotNull(captured.headers().lastHeader("traceId"));
        assertEquals("boot3-trace-789", new String(captured.headers().lastHeader("traceId").value(), StandardCharsets.UTF_8));
    }
}
