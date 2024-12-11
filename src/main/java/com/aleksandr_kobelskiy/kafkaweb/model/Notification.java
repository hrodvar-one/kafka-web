package com.aleksandr_kobelskiy.kafkaweb.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Table("notifications")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Notification {

    @Id
    private Long id;

    private LocalDateTime createdAt;
    private LocalDateTime modifiedAt;
    private LocalDateTime expirationDate;
    private String message;
    private String messageType; // INTERNAL/EXTERNAL
    private String error;
    private String userUid;
    private String notificationStatus; // NEW/COMPLETE
    private String triggerCode; // USER_REGISTRATION_1/DELETE_USER_2
    private String objectType; // USER/PAYMENT/MERCHANT
    private String objectId; // UUID of user/payment/etc.
    private String subject; // PAYMENT SUCCESS/etc.
    private String createdBy; // SYSTEM/OPERATOR
    private Boolean hasConfirmOtp;
}
