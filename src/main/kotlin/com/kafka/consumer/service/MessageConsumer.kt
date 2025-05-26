package com.kafka.consumer.service

import com.kafka.consumer.config.KafkaProperties
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Message processing result
 */
data class ProcessingResult(
    val success: Boolean,
    val message: String,
    val timestamp: LocalDateTime = LocalDateTime.now(),
    val processingTime: Long = 0L
)

/**
 * Kafka message consumer service
 * 
 * This service handles incoming Kafka messages with comprehensive error handling,
 * retry mechanisms, and monitoring capabilities.
 */
@Service
class MessageConsumer(
    private val kafkaProperties: KafkaProperties,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    
    private val processedCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)
    
    /**
     * Main message consumer with retry capability
     */
    @RetryableTopic(
        attempts = "\${kafka.retry.attempts:3}",
        backoff = Backoff(delayExpression = "\${kafka.retry.backoff-delay:1000}"),
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR,
        retryTopicSuffix = "-retry",
        dltTopicSuffix = "-dlt"
    )
    @KafkaListener(
        topics = ["\${kafka.topics.input-topic:input-messages}"],
        groupId = "\${kafka.consumer.group-id:kafka-consumer-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun consumeMessage(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long,
        record: ConsumerRecord<String, String>,
        acknowledgment: Acknowledgment
    ) {
        val startTime = System.currentTimeMillis()
        
        try {
            logger.info { 
                "Consuming message from topic: $topic, partition: $partition, " +
                "offset: $offset, timestamp: $timestamp, key: ${record.key()}" 
            }
            
            val result = processMessage(message, record.key())
            
            if (result.success) {
                processedCount.incrementAndGet()
                acknowledgment.acknowledge()
                
                val processingTime = System.currentTimeMillis() - startTime
                logger.info { 
                    "Successfully processed message. Count: ${processedCount.get()}, " +
                    "Processing time: ${processingTime}ms" 
                }
            } else {
                errorCount.incrementAndGet()
                logger.error { "Failed to process message: ${result.message}" }
                throw MessageProcessingException("Processing failed: ${result.message}")
            }
            
        } catch (exception: Exception) {
            errorCount.incrementAndGet()
            logger.error(exception) { 
                "Error processing message from topic: $topic, partition: $partition, offset: $offset" 
            }
            throw exception
        }
    }
    
    /**
     * Dead Letter Topic (DLT) handler for messages that failed all retry attempts
     */
    @KafkaListener(
        topics = ["\${kafka.topics.input-topic:input-messages}-dlt"],
        groupId = "\${kafka.consumer.group-id:kafka-consumer-group}-dlt"
    )
    fun handleDltMessage(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) exceptionMessage: String?,
        record: ConsumerRecord<String, String>,
        acknowledgment: Acknowledgment
    ) {
        logger.error { 
            "Message sent to DLT. Topic: $topic, Key: ${record.key()}, " +
            "Exception: $exceptionMessage, Message: $message" 
        }
        
        // Send to error topic for further analysis
        try {
            val errorMessage = "DLT_MESSAGE: $message | EXCEPTION: $exceptionMessage | ORIGINAL_TOPIC: $topic"
            kafkaTemplate.send(kafkaProperties.topics.errorTopic, record.key(), errorMessage)
            acknowledgment.acknowledge()
            
            logger.info { "DLT message forwarded to error topic: ${kafkaProperties.topics.errorTopic}" }
        } catch (exception: Exception) {
            logger.error(exception) { "Failed to forward DLT message to error topic" }
        }
    }
    
    /**
     * Process individual message with business logic
     */
    private fun processMessage(message: String, key: String?): ProcessingResult {
        return try {
            // Simulate message processing
            when {
                message.isBlank() -> {
                    ProcessingResult(false, "Empty message received")
                }
                message.contains("ERROR", ignoreCase = true) -> {
                    ProcessingResult(false, "Message contains error indicator")
                }
                message.contains("FAIL", ignoreCase = true) -> {
                    throw MessageProcessingException("Simulated processing failure")
                }
                else -> {
                    // Simulate processing time
                    Thread.sleep(100)
                    
                    logger.debug { "Processing message with key: $key, content: $message" }
                    ProcessingResult(true, "Message processed successfully")
                }
            }
        } catch (exception: Exception) {
            ProcessingResult(false, "Processing exception: ${exception.message}")
        }
    }
    
    /**
     * Get processing statistics
     */
    fun getProcessingStats(): Map<String, Any> {
        return mapOf(
            "processedCount" to processedCount.get(),
            "errorCount" to errorCount.get(),
            "successRate" to calculateSuccessRate(),
            "timestamp" to LocalDateTime.now()
        )
    }
    
    private fun calculateSuccessRate(): Double {
        val total = processedCount.get() + errorCount.get()
        return if (total > 0) {
            (processedCount.get().toDouble() / total.toDouble()) * 100.0
        } else {
            0.0
        }
    }
    
    /**
     * Reset processing statistics
     */
    fun resetStats() {
        processedCount.set(0)
        errorCount.set(0)
        logger.info { "Processing statistics reset" }
    }
}

/**
 * Custom exception for message processing failures
 */
class MessageProcessingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
