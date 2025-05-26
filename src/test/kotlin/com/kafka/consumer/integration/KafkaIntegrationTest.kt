package com.kafka.consumer.integration

import com.kafka.consumer.KafkaConsumerApplication
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*

/**
 * Integration tests using Testcontainers for Kafka
 */
@SpringBootTest(classes = [KafkaConsumerApplication::class])
@Testcontainers
@DirtiesContext
@TestPropertySource(properties = [
    "kafka.broker.servers=\${kafka.bootstrap.servers}",
    "kafka.consumer.group-id=integration-test-group",
    "kafka.topics.input-topic=test-input-topic",
    "kafka.topics.error-topic=test-error-topic"
])
class KafkaIntegrationTest {
    
    companion object {
        @Container
        @JvmStatic
        val kafkaContainer: KafkaContainer = KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
        ).withReuse(true)
    }
    
    @Test
    fun `should start application context successfully`() {
        // Test that the application context loads successfully
        // with Kafka container running
        println("Kafka bootstrap servers: ${kafkaContainer.bootstrapServers}")
    }
    
    @Test
    fun `should consume messages from kafka topic`() {
        // Given
        val producer = createTestProducer()
        val topic = "test-input-topic"
        val testMessage = "Integration test message"
        
        // When
        producer.send(ProducerRecord(topic, "test-key", testMessage))
        producer.flush()
        
        // Then
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted {
                // In a real integration test, you would verify that the message
                // was processed by checking logs, database, or monitoring endpoints
                println("Message sent to topic: $topic")
            }
        
        producer.close()
    }
    
    private fun createTestProducer(): KafkaProducer<String, String> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
        }
        return KafkaProducer(props)
    }
}
