package com.qwertcardo.kafka.listeners;

import com.qwertcardo.kafka.dto.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class EvenListenner {
    @KafkaListener(groupId = "even", topics = {"calculator_topic", "even"}, containerFactory = "containerFactory")
    public void evenListener(@Payload ConsumerRecord<Object, Message> record) throws Exception {

        if (record.value().getValor() % 2 == 0) {
            record.headers().add("even", "true".getBytes());
            throw new Exception("Even Number");
        }
    }

}
