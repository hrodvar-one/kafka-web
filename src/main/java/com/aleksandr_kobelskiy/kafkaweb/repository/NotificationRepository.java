package com.aleksandr_kobelskiy.kafkaweb.repository;

import com.aleksandr_kobelskiy.kafkaweb.model.Notification;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface NotificationRepository extends R2dbcRepository<Notification, Long> {

    // Получить все уведомления с пагинацией
    @Query("SELECT * FROM notifications LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}")
    Flux<Notification> findAllBy(Pageable pageable);

    // Найти уведомление по ID
    Mono<Notification> findById(Long id);

    // Найти все уведомления с определенным статусом
    @Query("SELECT * FROM notifications WHERE notification_status = :status")
    Flux<Notification> findAllByStatus(String status);

    // Обновить статус уведомления
    @Query("UPDATE notifications SET notification_status = :status, modified_at = now() WHERE id = :id")
    Mono<Void> updateStatus(Long id, String status);
}
