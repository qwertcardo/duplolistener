package com.qwertcardo.kafka.config;

import com.qwertcardo.kafka.dto.Message;
import com.qwertcardo.kafka.exception.FailedMessageProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
@Slf4j
public class KafkaConfig {
    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;
    @Value(value = "${kafka.retry.interval}")
    private Long retryInterval;
    @Value(value = "${kafka.retry.attempts}")
    private Integer retryAttempts;
    @Autowired
    private KafkaTemplate kafkaTemplate;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    if (r.headers().headers("even").iterator().hasNext()) {
                        return new TopicPartition("even", r.partition());
                    } else {
                        return new TopicPartition("odd", r.partition());
                    }
                });
    }

    public DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(retryInterval, retryAttempts);
        var errorHandler = new DefaultErrorHandler(publishingRecoverer(), fixedBackOff);

        errorHandler.setRetryListeners(((consumerRecord, error, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {}, deliveryAttempt ; {}",
                    error.getMessage(), deliveryAttempt);
        }));

        return errorHandler;
    }

    @Bean
    public ConsumerFactory<Object, Message> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_FUNCTION, FailedMessageProvider.class);

        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.KEY_DEFAULT_TYPE, Object.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.qwertcardo.kafka.dto.Message");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "containerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Message>
    evenKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setSyncCommits(Boolean.TRUE);
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }
}
