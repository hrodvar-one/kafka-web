package com.aleksandr_kobelskiy.kafkaweb.it;

import com.aleksandr_kobelskiy.kafkaweb.KafkaWebApplication;
import com.aleksandr_kobelskiy.kafkaweb.entity.*;
import com.aleksandr_kobelskiy.kafkaweb.repository.NotificationRepository;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = KafkaWebApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@Testcontainers

public class itNotificationRestControllerV1Test {

//    @Container
//    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
//            new PostgreSQLContainer<>("postgres:15-alpine")
//                    .withDatabaseName("testdb")
//                    .withUsername("testuser")
//                    .withPassword("testpass");

    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("testdb")
                    .withUsername("testuser")
                    .withPassword("testpass");

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer("confluentinc/cp-kafka:7.0.1");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.enabled", () -> "false");
        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://" + POSTGRES_CONTAINER.getHost() + ":" + POSTGRES_CONTAINER.getMappedPort(5432) + "/" + POSTGRES_CONTAINER.getDatabaseName());
        registry.add("spring.r2dbc.username", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES_CONTAINER::getPassword);

        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);
    }

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private NotificationRepository notificationRepository;

    @BeforeAll
    static void startContainer() {
        POSTGRES_CONTAINER.start();
    }

    @BeforeEach
    void setUp() {
        notificationRepository.deleteAll().block(); // Очищаем данные перед каждым тестом
    }

    @AfterEach
    void tearDown() {
        notificationRepository.deleteAll().block(); // Очищаем данные после каждого теста
    }

    @AfterAll
    static void stopContainer() {
        POSTGRES_CONTAINER.stop();
    }

    @Test
    void getAllNotifications_success() {
        NotificationEntity notification = NotificationEntity.builder()
                .createdAt(LocalDateTime.now())
                .message("Test message")
                .userUid("123")
                .notificationStatus(NotificationStatus.NEW)
                .hasConfirmOtp(false)
                .build();
        notificationRepository.save(notification).block();

        webTestClient.get()
                .uri("/api/v1/notifications")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(NotificationEntity.class)
                .hasSize(1)
                .value(notifications -> assertThat(notifications).isNotEmpty());
    }

//    @Test
//    void getNotificationById_success() {
//        NotificationEntity notification = NotificationEntity.builder()
//                .createdAt(LocalDateTime.now())
//                .message("Test message")
//                .messageType(MessageType.INTERNAL)
//                .userUid("123")
//                .notificationStatus(NotificationStatus.NEW)
//                .triggerCode(TriggerCode.USER_REGISTRATION_1)
//                .objectType(ObjectType.USER)
//                .objectId("uuid123")
//                .subject("Payment Success")
//                .createdBy(CreatedBy.SYSTEM)
//                .hasConfirmOtp(false)
//                .build();
//
//        // Сохраняем объект в репозиторий
//        NotificationEntity savedNotification = notificationRepository.save(notification).block();
//
//        // Выполняем запрос к контроллеру
//        webTestClient.get()
//                .uri("/api/v1/notifications/" + savedNotification.getId())
//                .exchange()
//                .expectStatus().isOk()
//                .expectBody(NotificationEntity.class)
//                .value(response -> assertThat(response.getId()).isEqualTo(savedNotification.getId()));
//    }
//
//    @Test
//    void updateStatus_success() {
//        NotificationEntity notification = new NotificationEntity(
//                null,
//                LocalDateTime.now(),
//                null,
//                null,
//                "Test message",
//                null,
//                null,
//                "123",
//                NotificationStatus.NEW,
//                null,
//                null,
//                null,
//                null,
//                false
//        );
//        NotificationEntity savedNotification = notificationRepository.save(notification).block();
//
//        webTestClient.patch()
//                .uri("/api/v1/notifications/" + savedNotification.getId() + "/status?status=COMPLETE")
//                .exchange()
//                .expectStatus().isNoContent();
//
//        NotificationEntity updatedNotification = notificationRepository.findById(savedNotification.getId()).block();
//        assertThat(updatedNotification.getNotificationStatus()).isEqualTo(NotificationStatus.COMPLETE);
//    }
//
//    @Test
//    void getAllNotifications_failure() {
//        webTestClient.get()
//                .uri("/api/v1/notifications")
//                .exchange()
//                .expectStatus().isOk()
//                .expectBodyList(NotificationEntity.class)
//                .hasSize(0);
//    }
}
