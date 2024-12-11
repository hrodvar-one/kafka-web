package com.aleksandr_kobelskiy.kafkaweb.rest;

import com.aleksandr_kobelskiy.kafkaweb.model.Notification;
import com.aleksandr_kobelskiy.kafkaweb.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/notifications")
public class NotificationRestControllerV1 {

    private final NotificationService notificationService;

    @GetMapping
    public Flux<Notification> getAllNotifications(Pageable pageable) {
        return notificationService.getAllNotifications(pageable);
    }

    @GetMapping("/{id}")
    public Mono<Notification> getNotificationById(@PathVariable Long id) {
        return notificationService.getNotificationById(id);
    }

    @PatchMapping("/{id}/status")
    public Mono<Void> updateStatus(@PathVariable Long id, @RequestParam String status) {
        return notificationService.updateNotificationStatus(id, status);
    }
}
