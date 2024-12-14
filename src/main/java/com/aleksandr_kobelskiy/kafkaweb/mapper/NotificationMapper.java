package com.aleksandr_kobelskiy.kafkaweb.mapper;
import com.aleksandr_kobelskiy.kafkaweb.dto.NotificationDto;
import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import org.springframework.stereotype.Component;

@Component
public class NotificationMapper {
    public NotificationDto toDto(NotificationEntity entity) {
        if (entity == null) {
            return null;
        }
        NotificationDto dto = new NotificationDto();
        dto.setId(entity.getId());
        dto.setCreatedAt(entity.getCreatedAt());
        dto.setModifiedAt(entity.getModifiedAt());
        dto.setExpirationDate(entity.getExpirationDate());
        dto.setMessage(entity.getMessage());
        dto.setMessageType(entity.getMessageType());
        dto.setError(entity.getError());
        dto.setUserUid(entity.getUserUid());
        dto.setNotificationStatus(entity.getNotificationStatus());
        dto.setTriggerCode(entity.getTriggerCode());
        dto.setObjectType(entity.getObjectType());
        dto.setObjectId(entity.getObjectId());
        dto.setSubject(entity.getSubject());
        dto.setCreatedBy(entity.getCreatedBy());
        dto.setHasConfirmOtp(entity.getHasConfirmOtp());
        return dto;
    }
    public NotificationEntity toEntity(NotificationDto dto) {
        if (dto == null) {
            return null;
        }
        NotificationEntity entity = new NotificationEntity();
        entity.setId(dto.getId());
        entity.setCreatedAt(dto.getCreatedAt());
        entity.setModifiedAt(dto.getModifiedAt());
        entity.setExpirationDate(dto.getExpirationDate());
        entity.setMessage(dto.getMessage());
        entity.setMessageType(dto.getMessageType());
        entity.setError(dto.getError());
        entity.setUserUid(dto.getUserUid());
        entity.setNotificationStatus(dto.getNotificationStatus());
        entity.setTriggerCode(dto.getTriggerCode());
        entity.setObjectType(dto.getObjectType());
        entity.setObjectId(dto.getObjectId());
        entity.setSubject(dto.getSubject());
        entity.setCreatedBy(dto.getCreatedBy());
        entity.setHasConfirmOtp(dto.getHasConfirmOtp());
        return entity;
    }
}
