package com.kafka.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka

/**
 * Main application class for Kafka Consumer
 * 
 * This Spring Boot application provides Kafka message consumption capabilities
 * with comprehensive configuration and monitoring features.
 */
@SpringBootApplication
@EnableKafka
class KafkaConsumerApplication

fun main(args: Array<String>) {
    runApplication<KafkaConsumerApplication>(*args)
}
