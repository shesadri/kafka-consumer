package com.kafka.consumer.service

import com.kafka.consumer.config.KafkaProperties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for MessageConsumer service
 */
@ExtendWith(MockitoExtension::class)
class MessageConsumerTest {
    
    @Mock
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    
    @Mock
    private lateinit var acknowledgment: Acknowledgment
    
    private lateinit var kafkaProperties: KafkaProperties
    private lateinit var messageConsumer: MessageConsumer
    
    @BeforeEach
    fun setUp() {
        kafkaProperties = KafkaProperties(
            broker = KafkaProperties.BrokerConfig(servers = "localhost:9092"),
            consumer = KafkaProperties.ConsumerConfig(groupId = "test-group"),
            topics = KafkaProperties.TopicConfig(
                inputTopic = "test-input",
                errorTopic = "test-error",
                retryTopic = "test-retry"
            ),
            retry = KafkaProperties.RetryConfig(attempts = 3, backoffDelay = 1000L)
        )
        
        messageConsumer = MessageConsumer(kafkaProperties, kafkaTemplate)
    }
    
    @Test
    fun `should process valid message successfully`() {
        // Given
        val message = "Valid test message"
        val key = "test-key"
        val topic = "test-topic"
        val partition = 0
        val offset = 100L
        val timestamp = System.currentTimeMillis()
        
        val consumerRecord = ConsumerRecord(topic, partition, offset, key, message)
        
        // When
        messageConsumer.consumeMessage(
            message = message,
            topic = topic,
            partition = partition,
            offset = offset,
            timestamp = timestamp,
            record = consumerRecord,
            acknowledgment = acknowledgment
        )
        
        // Then
        verify(acknowledgment).acknowledge()
        val stats = messageConsumer.getProcessingStats()
        assertEquals(1L, stats["processedCount"])
        assertEquals(0L, stats["errorCount"])
    }
    
    @Test
    fun `should handle empty message`() {
        // Given
        val message = ""
        val key = "test-key"
        val topic = "test-topic"
        val partition = 0
        val offset = 100L
        val timestamp = System.currentTimeMillis()
        
        val consumerRecord = ConsumerRecord(topic, partition, offset, key, message)
        
        // When & Then
        try {
            messageConsumer.consumeMessage(
                message = message,
                topic = topic,
                partition = partition,
                offset = offset,
                timestamp = timestamp,
                record = consumerRecord,
                acknowledgment = acknowledgment
            )
        } catch (e: MessageProcessingException) {
            // Expected exception
        }
        
        verify(acknowledgment, never()).acknowledge()
        val stats = messageConsumer.getProcessingStats()
        assertEquals(0L, stats["processedCount"])
        assertEquals(1L, stats["errorCount"])
    }
    
    @Test
    fun `should handle message with error indicator`() {
        // Given
        val message = "This is an ERROR message"
        val key = "test-key"
        val topic = "test-topic"
        val partition = 0
        val offset = 100L
        val timestamp = System.currentTimeMillis()
        
        val consumerRecord = ConsumerRecord(topic, partition, offset, key, message)
        
        // When & Then
        try {
            messageConsumer.consumeMessage(
                message = message,
                topic = topic,
                partition = partition,
                offset = offset,
                timestamp = timestamp,
                record = consumerRecord,
                acknowledgment = acknowledgment
            )
        } catch (e: MessageProcessingException) {
            // Expected exception
        }
        
        verify(acknowledgment, never()).acknowledge()
        val stats = messageConsumer.getProcessingStats()
        assertEquals(0L, stats["processedCount"])
        assertEquals(1L, stats["errorCount"])
    }
    
    @Test
    fun `should handle DLT message and forward to error topic`() {
        // Given
        val message = "DLT test message"
        val key = "dlt-key"
        val topic = "test-input-dlt"
        val exceptionMessage = "Processing failed after retries"
        
        val consumerRecord = ConsumerRecord(topic, 0, 100L, key, message)
        
        // When
        messageConsumer.handleDltMessage(
            message = message,
            topic = topic,
            exceptionMessage = exceptionMessage,
            record = consumerRecord,
            acknowledgment = acknowledgment
        )
        
        // Then
        verify(kafkaTemplate).send(
            eq(kafkaProperties.topics.errorTopic),
            eq(key),
            contains(message)
        )
        verify(acknowledgment).acknowledge()
    }
    
    @Test
    fun `should calculate success rate correctly`() {
        // Given - Process multiple messages
        val validMessage = "Valid message"
        val errorMessage = "ERROR message"
        val topic = "test-topic"
        val partition = 0
        val timestamp = System.currentTimeMillis()
        
        // Process one successful message
        val validRecord = ConsumerRecord(topic, partition, 100L, "key1", validMessage)
        messageConsumer.consumeMessage(
            message = validMessage,
            topic = topic,
            partition = partition,
            offset = 100L,
            timestamp = timestamp,
            record = validRecord,
            acknowledgment = acknowledgment
        )
        
        // Process one error message
        val errorRecord = ConsumerRecord(topic, partition, 101L, "key2", errorMessage)
        try {
            messageConsumer.consumeMessage(
                message = errorMessage,
                topic = topic,
                partition = partition,
                offset = 101L,
                timestamp = timestamp,
                record = errorRecord,
                acknowledgment = acknowledgment
            )
        } catch (e: MessageProcessingException) {
            // Expected
        }
        
        // When
        val stats = messageConsumer.getProcessingStats()
        
        // Then
        assertEquals(1L, stats["processedCount"])
        assertEquals(1L, stats["errorCount"])
        assertEquals(50.0, stats["successRate"] as Double, 0.1)
    }
    
    @Test
    fun `should reset statistics correctly`() {
        // Given - Process a message first
        val message = "Test message"
        val topic = "test-topic"
        val consumerRecord = ConsumerRecord(topic, 0, 100L, "key", message)
        
        messageConsumer.consumeMessage(
            message = message,
            topic = topic,
            partition = 0,
            offset = 100L,
            timestamp = System.currentTimeMillis(),
            record = consumerRecord,
            acknowledgment = acknowledgment
        )
        
        // Verify initial stats
        var stats = messageConsumer.getProcessingStats()
        assertEquals(1L, stats["processedCount"])
        
        // When
        messageConsumer.resetStats()
        
        // Then
        stats = messageConsumer.getProcessingStats()
        assertEquals(0L, stats["processedCount"])
        assertEquals(0L, stats["errorCount"])
        assertEquals(0.0, stats["successRate"] as Double, 0.1)
    }
    
    @Test
    fun `should handle processing exception gracefully`() {
        // Given
        val message = "FAIL message" // This will trigger an exception
        val key = "test-key"
        val topic = "test-topic"
        val partition = 0
        val offset = 100L
        val timestamp = System.currentTimeMillis()
        
        val consumerRecord = ConsumerRecord(topic, partition, offset, key, message)
        
        // When & Then
        try {
            messageConsumer.consumeMessage(
                message = message,
                topic = topic,
                partition = partition,
                offset = offset,
                timestamp = timestamp,
                record = consumerRecord,
                acknowledgment = acknowledgment
            )
        } catch (e: MessageProcessingException) {
            assertTrue(e.message?.contains("Simulated processing failure") == true)
        }
        
        verify(acknowledgment, never()).acknowledge()
        val stats = messageConsumer.getProcessingStats()
        assertEquals(0L, stats["processedCount"])
        assertEquals(1L, stats["errorCount"])
    }
}
