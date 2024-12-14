package com.aleksandr_kobelskiy.kafkaweb.service;

import com.aleksandr_kobelskiy.kafkaweb.dto.NotificationDto;
import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationStatus;
import com.aleksandr_kobelskiy.kafkaweb.mapper.NotificationMapper;
import com.aleksandr_kobelskiy.kafkaweb.repository.NotificationRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.apache.kafka.streams.kstream.EmitStrategy.log;

@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRepository repository;
    private final NotificationMapper mapper;

    public void processMessage(String message) {
        NotificationDto notificationDto = parseMessage(message);

        if (notificationDto == null) {
            throw new IllegalArgumentException("Parsed NotificationDto is null");
        }

        if (notificationDto.getExpirationDate() == null) {
            notificationDto.setExpirationDate(LocalDateTime.now().plusDays(7));
        }

        NotificationEntity notificationEntity = mapper.toEntity(notificationDto);

        repository.save(notificationEntity)
                .doOnSuccess(entity -> log.info("Notification saved successfully: {}", entity))
                .doOnError(error -> log.error("Error saving notification: {}", error.getMessage(), error))
                .subscribe();
    }

    public Flux<NotificationDto> getAllNotifications(Pageable pageable) {
        Sort sort = pageable.getSortOr(Sort.unsorted());
        return repository.findAll(sort)
                .map(mapper::toDto);// Убедитесь, что репозиторий принимает Sort
    }

    public Mono<NotificationDto> getNotificationById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Notification not found")))
                .map(mapper::toDto);
    }

    public Mono<Void> updateNotificationStatus(Long id, String status) {
        try {
            NotificationStatus notificationStatus = NotificationStatus.valueOf(status); // Проверка валидности статуса
            return repository.findById(id)
                    .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Notification not found")))
                    .flatMap(notificationEntity -> {
                        notificationEntity.setNotificationStatus(notificationStatus);
                        notificationEntity.setModifiedAt(LocalDateTime.now());
                        return repository.save(notificationEntity);
                    })
                    .then();
        } catch (IllegalArgumentException e) {
            // Исключение, если статус недопустим
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid notification status: " + status));
        }
    }

    private NotificationDto parseMessage(String message) {
        ObjectMapper objectMapper = new ObjectMapper();
        // Добавляем поддержку Java 8 дат и времени
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        try {
            NotificationEntity entity = objectMapper.readValue(message, NotificationEntity.class);
            return mapper.toDto(entity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse message: " + message, e);
        }
    }
}
