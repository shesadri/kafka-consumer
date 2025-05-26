# Kafka Consumer Application

A robust Kafka consumer application built with Kotlin, Spring Boot, and Gradle. This application provides comprehensive message consumption capabilities with error handling, retry mechanisms, and monitoring features.

## Features

- **Kafka Message Consumption**: Reliable message consumption from Kafka topics
- **Error Handling**: Comprehensive error handling with retry mechanisms
- **Dead Letter Topic (DLT)**: Failed messages are sent to DLT for analysis
- **Monitoring**: REST endpoints for monitoring processing statistics
- **Configuration**: Flexible configuration using application.yml
- **Testing**: Comprehensive unit and integration tests
- **Spring Boot Integration**: Full Spring Boot integration with auto-configuration

## Technology Stack

- **Kotlin**: Programming language
- **Spring Boot 3.2.0**: Application framework
- **Spring Kafka**: Kafka integration
- **Gradle**: Build tool with Kotlin DSL
- **JUnit 5**: Testing framework
- **Testcontainers**: Integration testing with real Kafka
- **Mockito**: Mocking framework

## Project Structure

```
src/
├── main/
│   ├── kotlin/
│   │   └── com/kafka/consumer/
│   │       ├── KafkaConsumerApplication.kt          # Main application class
│   │       ├── config/
│   │       │   └── KafkaConfig.kt                   # Kafka configuration
│   │       ├── service/
│   │       │   └── MessageConsumer.kt               # Consumer service
│   │       └── controller/
│   │           └── ConsumerController.kt            # REST endpoints
│   └── resources/
│       └── application.yml                          # Configuration file
└── test/
    ├── kotlin/
    │   └── com/kafka/consumer/
    │       ├── service/
    │       │   └── MessageConsumerTest.kt           # Unit tests
    │       ├── config/
    │       │   └── KafkaConfigTest.kt               # Configuration tests
    │       ├── controller/
    │       │   └── ConsumerControllerTest.kt        # Controller tests
    │       └── integration/
    │           └── KafkaIntegrationTest.kt          # Integration tests
    └── resources/
        └── application-test.yml                     # Test configuration
```

## Configuration

The application uses `application.yml` for configuration with support for multiple profiles:

### Kafka Configuration

- **Broker Settings**: Kafka broker servers, security protocol
- **Consumer Settings**: Group ID, offset reset policy, serializers
- **Topic Configuration**: Input, error, and retry topics
- **Retry Configuration**: Number of attempts and backoff delay

### Environment Variables

Key configuration can be overridden using environment variables:

- `KAFKA_BROKER_SERVERS`: Kafka broker servers
- `KAFKA_CONSUMER_GROUP_ID`: Consumer group ID
- `KAFKA_INPUT_TOPIC`: Input topic name
- `KAFKA_ERROR_TOPIC`: Error topic name
- `KAFKA_RETRY_TOPIC`: Retry topic name

## Building and Running

### Prerequisites

- Java 17 or higher
- Kafka cluster (local or remote)

### Build

```bash
./gradlew build
```

### Run Tests

```bash
./gradlew test
```

### Run Application

```bash
./gradlew bootRun
```

### Run with Custom Profile

```bash
./gradlew bootRun --args='--spring.profiles.active=dev'
```

## API Endpoints

The application provides REST endpoints for monitoring:

- `GET /kafka-consumer/api/v1/consumer/stats` - Get processing statistics
- `POST /kafka-consumer/api/v1/consumer/reset-stats` - Reset statistics
- `GET /kafka-consumer/api/v1/consumer/health` - Health check
- `GET /kafka-consumer/actuator/health` - Spring Boot health endpoint

## Message Processing

The consumer processes messages with the following features:

1. **Automatic Retry**: Failed messages are retried based on configuration
2. **Dead Letter Topic**: Messages that fail all retries are sent to DLT
3. **Error Handling**: Comprehensive error handling and logging
4. **Statistics**: Track processed and failed message counts
5. **Manual Acknowledgment**: Messages are acknowledged only after successful processing

## Testing

The project includes comprehensive tests:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test with real Kafka using Testcontainers
- **Configuration Tests**: Verify Kafka configuration setup
- **Controller Tests**: Test REST endpoints

### Running Integration Tests

Integration tests use Testcontainers to start a real Kafka instance:

```bash
./gradlew test --tests "*IntegrationTest"
```

## Monitoring and Observability

- **Processing Statistics**: Track success/failure rates
- **Health Checks**: Application and Kafka connectivity health
- **Metrics**: Spring Boot Actuator metrics
- **Logging**: Structured logging with configurable levels

## Error Handling Strategy

1. **Immediate Retry**: Automatic retry with exponential backoff
2. **Retry Topic**: Failed messages sent to retry topic
3. **Dead Letter Topic**: Final destination for unprocessable messages
4. **Error Topic**: Error messages for analysis and alerting

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License.
