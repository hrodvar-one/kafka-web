package com.aleksandr_kobelskiy.kafkaweb.service;

import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationEntity;
import com.aleksandr_kobelskiy.kafkaweb.entity.NotificationStatus;
import com.aleksandr_kobelskiy.kafkaweb.repository.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class NotificationServiceTest {
    private NotificationRepository repository;
    private NotificationService service;

    @BeforeEach
    void setUp() {
        repository = mock(NotificationRepository.class);
        service = new NotificationService(repository);
    }

    @Test
    void processMessage_success() {
        String message = "{\"message\":\"Test message\"}";
        NotificationEntity entity = new NotificationEntity();
        entity.setMessage("Test message");
        entity.setExpirationDate(LocalDateTime.now().plusDays(7));

        when(repository.save(any(NotificationEntity.class))).thenReturn(Mono.just(entity));

        assertDoesNotThrow(() -> service.processMessage(message));
        verify(repository, times(1)).save(any(NotificationEntity.class));
    }

    @Test
    void processMessage_failure() {
        String invalidMessage = "Invalid JSON";

        RuntimeException exception = assertThrows(RuntimeException.class, () -> service.processMessage(invalidMessage));
        assertTrue(exception.getMessage().contains("Failed to parse message"));
    }

    @Test
    void getAllNotifications_success() {
        NotificationEntity entity = new NotificationEntity();
        when(repository.findAll(Sort.unsorted())).thenReturn(Flux.just(entity)); // Явно передаем Sort.unsorted()

        Flux<NotificationEntity> notifications = service.getAllNotifications(Pageable.unpaged());
        assertNotNull(notifications); // Проверяем, что Flux не null
        assertEquals(1, notifications.collectList().block().size()); // Проверяем, что размер списка равен 1
    }

    @Test
    void getAllNotifications_failure() {
        when(repository.findAll(any(Sort.class))).thenReturn(Flux.error(new RuntimeException("Error fetching notifications"))); // Используем Sort вместо Pageable

        Flux<NotificationEntity> notifications = service.getAllNotifications(Pageable.unpaged()); // Pageable преобразуется в Sort
        RuntimeException exception = assertThrows(RuntimeException.class, notifications::blockLast);

        assertEquals("Error fetching notifications", exception.getMessage()); // Проверяем сообщение исключения
    }

    @Test
    void getNotificationById_success() {
        NotificationEntity entity = new NotificationEntity();
        entity.setId(1L);
        when(repository.findById(1L)).thenReturn(Mono.just(entity));

        Mono<NotificationEntity> result = service.getNotificationById(1L);
        assertEquals(1L, result.block().getId());
    }

    @Test
    void getNotificationById_failure() {
        when(repository.findById(1L)).thenReturn(Mono.empty());

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> service.getNotificationById(1L).block());
        assertEquals("404 NOT_FOUND \"Notification not found\"", exception.getMessage());
    }

    @Test
    void updateNotificationStatus_success() {
        NotificationEntity entity = new NotificationEntity();
        entity.setId(1L);
        entity.setNotificationStatus(NotificationStatus.NEW);

        when(repository.findById(1L)).thenReturn(Mono.just(entity));
        when(repository.save(any(NotificationEntity.class))).thenReturn(Mono.just(entity));

        assertDoesNotThrow(() -> service.updateNotificationStatus(1L, "COMPLETE").block()); // Явный вызов block()
        verify(repository, times(1)).save(any(NotificationEntity.class));
    }

    @Test
    void updateNotificationStatus_failure() {
        when(repository.findById(1L)).thenReturn(Mono.empty());

        ResponseStatusException exception = assertThrows(ResponseStatusException.class,
                () -> service.updateNotificationStatus(1L, "COMPLETE").block());

        assertEquals("404 NOT_FOUND \"Notification not found\"", exception.getMessage());
        verify(repository, never()).save(any(NotificationEntity.class));
    }
}