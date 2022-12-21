package com.qwertcardo.kafka.exception;

import com.qwertcardo.kafka.dto.Message;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

import java.util.function.Function;

public class FailedMessageProvider implements Function<FailedDeserializationInfo, Message> {
    @Override
    public Message apply(FailedDeserializationInfo info) {
        return new ErrorMessage(info);
    }
}
