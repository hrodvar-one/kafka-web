package com.aleksandr_kobelskiy.kafkaweb.dto;

import com.aleksandr_kobelskiy.kafkaweb.entity.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class NotificationDto {

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
