// JsonMapperService.java
package com.example.demo.Services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.demo.DTOS.TickerDTO;
import com.example.demo.DTOS.NewsDTO;
import jakarta.validation.Validator;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class JsonMapperService {

    private final ObjectMapper objectMapper;
    private final Validator validator;

    // Spring injects ObjectMapper and Validator automatically
    public JsonMapperService(ObjectMapper objectMapper, Validator validator) {
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    public <T> T mapAndValidate(String jsonMessage, Class<T> targetClass) throws Exception {
        T dto = objectMapper.readValue(jsonMessage, targetClass);

        // JSR-303 Validation Check
        Set violations = validator.validate(dto);
        if (!violations.isEmpty()) {
            // Log the bad message and throw an exception to potentially drop the message (DLQ)
            throw new IllegalArgumentException("Validation failed for DTO: " + violations.toString());
        }
        return dto;
    }
}