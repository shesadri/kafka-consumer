package com.kafka.consumer.config

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer

private val logger = KotlinLogging.logger {}

/**
 * Kafka configuration properties
 */
@ConfigurationProperties(prefix = "kafka")
data class KafkaProperties(
    val broker: BrokerConfig = BrokerConfig(),
    val consumer: ConsumerConfig = ConsumerConfig(),
    val topics: TopicConfig = TopicConfig(),
    val retry: RetryConfig = RetryConfig()
) {
    data class BrokerConfig(
        val servers: String = "localhost:9092",
        val securityProtocol: String = "PLAINTEXT",
        val saslMechanism: String = "PLAIN"
    )
    
    data class ConsumerConfig(
        val groupId: String = "kafka-consumer-group",
        val autoOffsetReset: String = "earliest",
        val enableAutoCommit: Boolean = true,
        val autoCommitInterval: Int = 1000,
        val keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
        val valueDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
        val maxPollRecords: Int = 500,
        val sessionTimeout: Int = 30000,
        val heartbeatInterval: Int = 3000
    )
    
    data class TopicConfig(
        val inputTopic: String = "input-messages",
        val errorTopic: String = "error-messages",
        val retryTopic: String = "retry-messages"
    )
    
    data class RetryConfig(
        val attempts: Int = 3,
        val backoffDelay: Long = 1000L
    )
}

/**
 * Kafka configuration class
 */
@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfig(private val kafkaProperties: KafkaProperties) {

    /**
 * Consumer factory bean for creating Kafka consumers
     */
    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        logger.info { "Configuring Kafka consumer factory with broker: ${kafkaProperties.broker.servers}" }
        
        val configProps = mutableMapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.broker.servers,
            ConsumerConfig.GROUP_ID_CONFIG to kafkaProperties.consumer.groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to kafkaProperties.consumer.autoOffsetReset,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to kafkaProperties.consumer.enableAutoCommit,
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to kafkaProperties.consumer.autoCommitInterval,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS to StringDeserializer::class.java,
            ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to StringDeserializer::class.java,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to kafkaProperties.consumer.maxPollRecords,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to kafkaProperties.consumer.sessionTimeout,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to kafkaProperties.consumer.heartbeatInterval,
            JsonDeserializer.TRUSTED_PACKAGES to "*"
        )

        // Add security configuration if not PLAINTEXT
        if (kafkaProperties.broker.securityProtocol != "PLAINTEXT") {
            configProps["security.protocol"] = kafkaProperties.broker.securityProtocol
            configProps["sasl.mechanism"] = kafkaProperties.broker.saslMechanism
        }

        return DefaultKafkaConsumerFactory(configProps)
    }

    /**
     * Kafka listener container factory for managing consumer containers
     */
    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory()
        
        // Configure container properties
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.containerProperties.pollTimeout = 3000
        
        // Configure concurrency
        factory.setConcurrency(3)
        
        logger.info { "Kafka listener container factory configured successfully" }
        return factory
    }
}
