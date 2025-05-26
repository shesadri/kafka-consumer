package com.kafka.consumer.controller

import com.kafka.consumer.service.MessageConsumer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.InjectMocks
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Unit tests for ConsumerController
 */
@ExtendWith(MockitoExtension::class)
class ConsumerControllerTest {
    
    @Mock
    private lateinit var messageConsumer: MessageConsumer
    
    @InjectMocks
    private lateinit var consumerController: ConsumerController
    
    @Test
    fun `should return processing stats`() {
        // Given
        val expectedStats = mapOf(
            "processedCount" to 100L,
            "errorCount" to 5L,
            "successRate" to 95.0,
            "timestamp" to LocalDateTime.now()
        )
        whenever(messageConsumer.getProcessingStats()).thenReturn(expectedStats)
        
        // When
        val response = consumerController.getProcessingStats()
        
        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(expectedStats, response.body)
        verify(messageConsumer).getProcessingStats()
    }
    
    @Test
    fun `should reset statistics successfully`() {
        // When
        val response = consumerController.resetStats()
        
        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("Statistics reset successfully", response.body?.["message"])
        verify(messageConsumer).resetStats()
    }
    
    @Test
    fun `should return health check status`() {
        // When
        val response = consumerController.healthCheck()
        
        // Then
        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("UP", response.body?.["status"])
        assertEquals("kafka-consumer", response.body?.["service"])
    }
}
