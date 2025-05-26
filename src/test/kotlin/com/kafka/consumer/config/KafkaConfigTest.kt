package com.kafka.consumer.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Unit tests for Kafka configuration
 */
class KafkaConfigTest {
    
    @Test
    fun `should create kafka properties with default values`() {
        // When
        val kafkaProperties = KafkaProperties()
        
        // Then
        assertEquals("localhost:9092", kafkaProperties.broker.servers)
        assertEquals("kafka-consumer-group", kafkaProperties.consumer.groupId)
        assertEquals("earliest", kafkaProperties.consumer.autoOffsetReset)
        assertEquals("input-messages", kafkaProperties.topics.inputTopic)
        assertEquals("error-messages", kafkaProperties.topics.errorTopic)
        assertEquals("retry-messages", kafkaProperties.topics.retryTopic)
        assertEquals(3, kafkaProperties.retry.attempts)
        assertEquals(1000L, kafkaProperties.retry.backoffDelay)
    }
    
    @Test
    fun `should create consumer factory with correct configuration`() {
        // Given
        val kafkaProperties = KafkaProperties(
            broker = KafkaProperties.BrokerConfig(servers = "test-server:9092"),
            consumer = KafkaProperties.ConsumerConfig(
                groupId = "test-group",
                autoOffsetReset = "latest",
                enableAutoCommit = false
            )
        )
        val kafkaConfig = KafkaConfig(kafkaProperties)
        
        // When
        val consumerFactory = kafkaConfig.consumerFactory()
        
        // Then
        assertNotNull(consumerFactory)
        val configurationProperties = consumerFactory.configurationProperties
        
        assertEquals("test-server:9092", configurationProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("test-group", configurationProperties[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals("latest", configurationProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG])
        assertEquals(false, configurationProperties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(ErrorHandlingDeserializer::class.java, configurationProperties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG])
        assertEquals(ErrorHandlingDeserializer::class.java, configurationProperties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG])
    }
    
    @Test
    fun `should create kafka listener container factory`() {
        // Given
        val kafkaProperties = KafkaProperties()
        val kafkaConfig = KafkaConfig(kafkaProperties)
        
        // When
        val containerFactory = kafkaConfig.kafkaListenerContainerFactory()
        
        // Then
        assertNotNull(containerFactory)
        assertNotNull(containerFactory.consumerFactory)
        assertEquals(3, containerFactory.concurrency)
    }
    
    @Test
    fun `should configure security properties when not plaintext`() {
        // Given
        val kafkaProperties = KafkaProperties(
            broker = KafkaProperties.BrokerConfig(
                servers = "secure-server:9092",
                securityProtocol = "SASL_SSL",
                saslMechanism = "SCRAM-SHA-256"
            )
        )
        val kafkaConfig = KafkaConfig(kafkaProperties)
        
        // When
        val consumerFactory = kafkaConfig.consumerFactory()
        
        // Then
        val configurationProperties = consumerFactory.configurationProperties
        assertEquals("SASL_SSL", configurationProperties["security.protocol"])
        assertEquals("SCRAM-SHA-256", configurationProperties["sasl.mechanism"])
    }
}
