package com.kafka.consumer.controller

import com.kafka.consumer.service.MessageConsumer
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * REST controller for consumer monitoring and management
 */
@RestController
@RequestMapping("/api/v1/consumer")
class ConsumerController(private val messageConsumer: MessageConsumer) {
    
    @GetMapping("/stats")
    fun getProcessingStats(): ResponseEntity<Map<String, Any>> {
        return ResponseEntity.ok(messageConsumer.getProcessingStats())
    }
    
    @PostMapping("/reset-stats")
    fun resetStats(): ResponseEntity<Map<String, String>> {
        messageConsumer.resetStats()
        return ResponseEntity.ok(mapOf("message" to "Statistics reset successfully"))
    }
    
    @GetMapping("/health")
    fun healthCheck(): ResponseEntity<Map<String, String>> {
        return ResponseEntity.ok(mapOf(
            "status" to "UP",
            "service" to "kafka-consumer"
        ))
    }
}
