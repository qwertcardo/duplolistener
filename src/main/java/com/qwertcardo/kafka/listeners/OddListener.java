package com.qwertcardo.kafka.listeners;

import com.qwertcardo.kafka.dto.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OddListener {

    @KafkaListener(groupId = "odd", topics = {"calculator_topic", "odd"}, containerFactory = "containerFactory")
    public void oddListener(@Payload ConsumerRecord<Object, Message> record) throws Exception {

        if (record.value().getValor() % 2 != 0) {
            record.headers().add("odd", "true".getBytes());
            throw new Exception("Odd Number");
        }
    }
}
