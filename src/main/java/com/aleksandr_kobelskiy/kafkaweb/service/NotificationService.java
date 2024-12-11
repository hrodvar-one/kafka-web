package com.aleksandr_kobelskiy.kafkaweb.service;

import com.aleksandr_kobelskiy.kafkaweb.model.Notification;
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
        Notification notification = parseMessage(message);
        repository.save(notification).subscribe();
    }

    public Flux<Notification> getAllNotifications(Pageable pageable) {
        return repository.findAllBy(pageable);
    }

    public Mono<Notification> getNotificationById(Long id) {
        return repository.findById(id);
    }

    public Mono<Void> updateNotificationStatus(Long id, String status) {
        return repository.findById(id)
                .flatMap(notification -> {
                    notification.setNotificationStatus(status);
                    notification.setModifiedAt(LocalDateTime.now());
                    return repository.save(notification);
                })
                .then();
    }

    private Notification parseMessage(String message) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(message, Notification.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse message: " + message, e);
        }
    }
}
