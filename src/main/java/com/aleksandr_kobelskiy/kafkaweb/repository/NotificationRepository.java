package com.aleksandr_kobelskiy.kafkaweb.repository;

import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface NotificationRepository extends R2dbcRepository<NotificationEntity, Long> {

    // Получить все уведомления с пагинацией
    @Query("SELECT * FROM notifications LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}")
    Flux<NotificationEntity> findAllBy(Pageable pageable);

    // Найти уведомление по ID
    Mono<NotificationEntity> findById(Long id);

    // Найти все уведомления с определенным статусом
    @Query("SELECT * FROM notifications WHERE notification_status = :status")
    Flux<NotificationEntity> findAllByStatus(String status);

    // Обновить статус уведомления
    @Query("UPDATE notifications SET notification_status = :status, modified_at = now() WHERE id = :id")
    Mono<Void> updateStatus(Long id, String status);
}
