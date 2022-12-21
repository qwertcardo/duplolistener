package com.qwertcardo.kafka.exception;

import com.qwertcardo.kafka.dto.Message;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

public class ErrorMessage extends Message {
    private final FailedDeserializationInfo failedDeserializationInfo;
    public ErrorMessage(FailedDeserializationInfo failedDeserializationInfo) {
        this.failedDeserializationInfo = failedDeserializationInfo;
    }
    public FailedDeserializationInfo getFailedDeserializationInfo() {
        return this.failedDeserializationInfo;
    }
}
