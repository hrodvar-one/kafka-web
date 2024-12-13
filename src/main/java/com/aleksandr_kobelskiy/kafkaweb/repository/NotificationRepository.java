package com.aleksandr_kobelskiy.kafkaweb.repository;

import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;

public interface NotificationRepository extends R2dbcRepository<NotificationEntity, Long> {

}
