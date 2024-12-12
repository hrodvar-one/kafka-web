package com.aleksandr_kobelskiy.kafkaweb.service;

import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationStatus;
import com.aleksandr_kobelskiy.kafkaweb.repository.NotificationRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRepository repository;

    public void processMessage(String message) {
        NotificationEntity notificationEntity = parseMessage(message);
        repository.save(notificationEntity).subscribe();
    }

    public Flux<NotificationEntity> getAllNotifications(Pageable pageable) {
        return repository.findAllBy(pageable);
    }

    public Mono<NotificationEntity> getNotificationById(Long id) {
        return repository.findById(id);
    }

    public Mono<Void> updateNotificationStatus(Long id, String status) {
        return repository.findById(id)
                .flatMap(notificationEntity -> {
//                    notificationEntity.setNotificationStatus(status);
                    notificationEntity.setNotificationStatus(NotificationStatus.valueOf(status));
                    notificationEntity.setModifiedAt(LocalDateTime.now());
                    return repository.save(notificationEntity);
                })
                .then();
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
