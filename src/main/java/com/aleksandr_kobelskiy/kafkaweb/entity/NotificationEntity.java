package com.aleksandr_kobelskiy.kafkaweb.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Table("notifications")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationEntity {

    @Id
    private Long id;

    private LocalDateTime createdAt;
    private LocalDateTime modifiedAt;
    private LocalDateTime expirationDate;
    private String message;
    private MessageType messageType; // INTERNAL/EXTERNAL
    private String error;
    private String userUid;
    private NotificationStatus notificationStatus; // NEW/COMPLETE
    private TriggerCode triggerCode; // USER_REGISTRATION_1/DELETE_USER_2
    private ObjectType objectType; // USER/PAYMENT/MERCHANT
    private String objectId; // UUID of user/payment/etc.
    private String subject; // PAYMENT SUCCESS/etc.
    private CreatedBy createdBy; // SYSTEM/OPERATOR
    private Boolean hasConfirmOtp;
}
