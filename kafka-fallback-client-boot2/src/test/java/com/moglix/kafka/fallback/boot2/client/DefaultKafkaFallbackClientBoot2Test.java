package com.moglix.kafka.fallback.boot2.client;

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
import org.springframework.util.concurrent.SettableListenableFuture;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultKafkaFallbackClientBoot2Test {

    private KafkaTemplate<?, ?> mockKafkaTemplate;
    private FallbackHttpClient mockFallbackHttpClient;
    private DefaultKafkaFallbackClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockKafkaTemplate = mock(KafkaTemplate.class);
        mockFallbackHttpClient = mock(FallbackHttpClient.class);

        KafkaFallbackProperties props = new KafkaFallbackProperties();
        props.setApplicationName("boot2-test-app");
        SimpleCircuitBreaker cb = new SimpleCircuitBreaker(3, 5000);

        client = new DefaultKafkaFallbackClient(
                props,
                "boot2-test-app",
                new ObjectMapper(),
                cb,
                mockFallbackHttpClient,
                mockKafkaTemplate
        );
    }

    @Test
    @DisplayName("Should send ProducerRecord to KafkaTemplate<?, ?> with custom headers")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSendToKafkaWithHeaders() throws Exception {
        SettableListenableFuture<SendResult> future = new SettableListenableFuture<>();
        ProducerRecord dummyRecord = new ProducerRecord("orders", "dummy");
        RecordMetadata meta = new RecordMetadata(new TopicPartition("orders", 0), 0, 0, 0, 0L, 0, 0);
        future.set(new SendResult(dummyRecord, meta));

        when(((KafkaTemplate) mockKafkaTemplate).send(any(ProducerRecord.class))).thenReturn(future);

        Map<String, String> headers = Collections.singletonMap("traceId", "test-trace-123");
        PublishResult result = client.publish("orders", "{\"orderId\":\"1\"}", headers);

        assertEquals(PublishStatus.SENT_TO_KAFKA, result.getStatus());
        assertTrue(result.isSentToKafka());

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(((KafkaTemplate) mockKafkaTemplate)).send(captor.capture());

        ProducerRecord captured = captor.getValue();
        assertEquals("orders", captured.topic());
        assertEquals("{\"orderId\":\"1\"}", captured.value());
        assertNotNull(captured.headers().lastHeader("traceId"));
        assertEquals("test-trace-123", new String(captured.headers().lastHeader("traceId").value(), StandardCharsets.UTF_8));
    }
}
