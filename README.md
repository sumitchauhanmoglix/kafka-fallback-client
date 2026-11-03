# Kafka Fallback Client Starter

A resilient Spring Boot starter for Apache Kafka producers with automatic failover to an HTTP fallback buffer service and built-in circuit breaking.

Supports both **Spring Boot 2** (Java 8+) and **Spring Boot 3** (Java 17+).

---

## 1. Architecture Overview

```
                      +-----------------------------+
                      |   KafkaFallbackOperations   |
                      |        .publish()           |
                      +--------------+--------------+
                                     |
                       [Fail-Fast Validation: Topic & JSON]
                                     |
                         +-----------v-----------+
                         |    Circuit Breaker    |
                         +-----+-----------+-----+
                               |           |
                     CLOSED    |           | OPEN / HALF-OPEN
                               v           v
                     +---------+---+   +---+---------+
                     |    Kafka    |   |  Fallback   |
                     |  Producer   |   | HTTP Client |
                     +---------+---+   +---+---------+
                               |           |
                        Kafka  |           |
                       Failure |           |
                               +---------->+ (POST /v1/messages)
                                           |
                                           v
                              +------------+------------+
                              | PublishResult(BUFFERED) |
                              +-------------------------+
```

1. **Pre-flight Validation**: Synchronously rejects null/blank topics, null/blank payloads, and non-valid JSON strings before any network call.
2. **Kafka First**: Dispatches to Kafka using configured timeouts (`max.block.ms`, `delivery.timeout.ms`) and `acks=all`.
3. **Resilience & Circuit Breaker**: If Kafka fails or the built-in circuit breaker is tripped `OPEN`, traffic immediately bypasses Kafka and routes directly to the HTTP Fallback service.
4. **Disaster Logging**: If both Kafka and the Fallback HTTP service fail, the event is logged with full payload context at `ERROR` level and a `KafkaFallbackException` is thrown.

---

## 2. Fallback Message Contract (`POST /v1/messages`)

When routing to the fallback service, the client sends a POST request matching the JIRA 1 specification:

```json
{
  "topic": "order-events",
  "applicationName": "order-service",
  "payload": {
    "orderId": "ORD-1001",
    "status": "CONFIRMED"
  },
  "headers": {
    "traceId": "abc-123"
  }
}
```

### Contract Fields:
- `topic` (`String`, required): Target Kafka topic name.
- `applicationName` (`String`, required): Resolved from `kafka.fallback.application-name` (defaults to `spring.application.name`).
- `payload` (`Object` / `JsonNode`, required): The actual JSON payload sent as-is and unmodified.
- `headers` (`Map<String, String>`, optional):
  > When no headers are provided (null or empty), the `headers` field is **completely omitted** from the JSON payload.

---

## 3. Dependency Setup

### For Spring Boot 2.x (Java 8, 11, 17)
```xml
<dependency>
    <groupId>com.moglix</groupId>
    <artifactId>kafka-fallback-client-boot2</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### For Spring Boot 3.x (Java 17, 21)
```xml
<dependency>
    <groupId>com.moglix</groupId>
    <artifactId>kafka-fallback-client-boot3</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 4. Quick Start & Public API

Inject `KafkaFallbackOperations` into your service:

```java
import com.moglix.kafka.fallback.client.KafkaFallbackOperations;
import com.moglix.kafka.fallback.model.PublishResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventService {

    @Autowired
    private KafkaFallbackOperations kafkaFallback;

    // 1. Synchronous publish with JSON String
    public void sendOrderJson(String orderJson) {
        PublishResult result = kafkaFallback.publish("order-events", orderJson);
        System.out.println("Status: " + result.getStatus()); // SENT_TO_KAFKA or BUFFERED
    }

    // 2. Synchronous publish with Java Object and custom headers
    public void sendOrder(OrderDto orderDto, String traceId) {
        Map<String, String> headers = Collections.singletonMap("traceId", traceId);
        PublishResult result = kafkaFallback.publish("order-events", orderDto, headers);
        
        if (result.isBuffered()) {
            System.out.println("Message buffered with ID: " + result.getMessageId());
        }
    }

    // 3. Asynchronous publish
    public CompletableFuture<PublishResult> sendOrderAsync(OrderDto orderDto) {
        return kafkaFallback.publishAsync("order-events", orderDto);
    }
}
```

---

## 5. Configuration Reference

All properties are configured under `kafka.fallback.*`:

| Property | Default | Description |
| :--- | :--- | :--- |
| `kafka.fallback.enabled` | `true` | Enable or disable the fallback starter |
| `kafka.fallback.application-name` | `${spring.application.name}` | Calling application name sent in fallback DTO |
| `kafka.fallback.api-url` | *(Required)* | HTTP fallback endpoint (e.g. `http://fallback-api/v1/messages`) |
| `kafka.fallback.api-key` | `""` | Optional API Key sent in `X-API-Key` HTTP header |
| `kafka.fallback.timeouts.kafka-send-timeout-ms` | `2000` | Max blocking timeout for Kafka delivery before fallback |
| `kafka.fallback.timeouts.api-connect-timeout-ms` | `1000` | HTTP connect timeout for fallback service |
| `kafka.fallback.timeouts.api-read-timeout-ms` | `3000` | HTTP read timeout for fallback service |
| `kafka.fallback.circuit-breaker.failure-threshold` | `5` | Consecutive Kafka failures before tripping breaker OPEN |
| `kafka.fallback.circuit-breaker.wait-duration-in-open-ms` | `10000` | Time in ms before testing Kafka with a HALF_OPEN probe |
| `kafka.fallback.retry.max-attempts` | `2` | Number of retry attempts on HTTP fallback call |
| `kafka.fallback.retry.backoff-ms` | `200` | Backoff multiplier in ms between HTTP retries |

Example `application.yml`:
```yaml
spring:
  application:
    name: order-service
  kafka:
    bootstrap-servers: localhost:9092

kafka:
  fallback:
    enabled: true
    api-url: https://fallback.internal.moglix.com/v1/messages
    api-key: secret-service-key-xyz
    timeouts:
      kafka-send-timeout-ms: 1500
      api-connect-timeout-ms: 1000
      api-read-timeout-ms: 2500
    circuit-breaker:
      failure-threshold: 3
      wait-duration-in-open-ms: 8000
```

---

## 6. Migration Guide (from `KafkaTemplate`)

If your service currently uses `KafkaTemplate`:

### Before:
```java
@Autowired
private KafkaTemplate<String, Object> kafkaTemplate;

public void send(String topic, OrderEvent event) {
    kafkaTemplate.send(topic, event);
}
```

### After:
```java
@Autowired
private KafkaFallbackOperations kafkaFallback;

public void send(String topic, OrderEvent event) {
    PublishResult result = kafkaFallback.publish(topic, event);
}
```

### Key Differences:
1. **Guaranteed Fallback**: If Kafka brokers become unreachable, `KafkaFallbackOperations` automatically buffers your message via the HTTP fallback API instead of throwing timeout exceptions or blocking Tomcat request threads.
2. **Circuit Breaking**: Repeated broker failures trip the breaker, shielding downstream Kafka cluster and preventing thread pool starvation.
3. **Identical Message Contracts**: Messages retain topic and headers in both paths.
