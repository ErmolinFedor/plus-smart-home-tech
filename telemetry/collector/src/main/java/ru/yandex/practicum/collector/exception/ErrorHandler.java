package ru.yandex.practicum.collector.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@Slf4j
@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler(MappingStrategyNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleStrategyNotFound(MappingStrategyNotFoundException e) {
        log.error("КРИТИЧЕСКАЯ ОШИБКА АРХИТЕКТУРЫ: {}", e.getMessage());
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(SerializationException.class)
    public ResponseEntity<Map<String, String>> handleSerializationError(SerializationException e) {
        log.error("СБОЙ СЕРИАЛИЗАЦИИ: {}, Причина: {}", e.getMessage(), e.getCause().getMessage());
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidationValidationError(MethodArgumentNotValidException e) {
        log.warn("ОШИБКА ВАЛИДАЦИИ ВХОДЯЩЕГО JSON: {}", e.getBindingResult().getFieldError().getField() + " "
                + e.getBindingResult().getFieldError().getDefaultMessage());
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "Invalid request content"));
    }

    @ExceptionHandler(Throwable.class)
    public ResponseEntity<Map<String, String>> handleGeneralException(Throwable e) {
        log.error("НЕПРЕДВИДЕННАЯ СИСТЕМНАЯ ОШИБКА: ", e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal Server Error"));
    }
}
