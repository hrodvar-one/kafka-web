package com.aleksandr_kobelskiy.kafkaweb.service;

import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationStatus;
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

    public void processMessage(String message) {
        NotificationEntity notificationEntity = parseMessage(message);

        // Проверяем, заполнено ли поле expirationDate
        if (notificationEntity.getExpirationDate() == null) {
            // Устанавливаем значение по умолчанию, например, 7 дней с текущего времени
            notificationEntity.setExpirationDate(LocalDateTime.now().plusDays(7));
        }

        repository.save(notificationEntity)
                .doOnSuccess(entity -> log.info("Notification saved successfully: {}", entity))
                .doOnError(error -> log.error("Error saving notification: {}", error.getMessage(), error))
                .subscribe();
    }

    public Flux<NotificationEntity> getAllNotifications(Pageable pageable) {
        Sort sort = pageable.getSortOr(Sort.unsorted());
        return repository.findAll(sort); // Убедитесь, что репозиторий принимает Sort
    }

    public Mono<NotificationEntity> getNotificationById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Notification not found")));
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

    private NotificationEntity parseMessage(String message) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(message, NotificationEntity.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse message: " + message, e);
        }
    }
}
