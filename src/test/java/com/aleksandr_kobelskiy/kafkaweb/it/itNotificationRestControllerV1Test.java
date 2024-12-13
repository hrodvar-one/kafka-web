package com.aleksandr_kobelskiy.kafkaweb.it;

import com.aleksandr_kobelskiy.kafkaweb.KafkaWebApplication;
import com.aleksandr_kobelskiy.kafkaweb.entity.*;
import com.aleksandr_kobelskiy.kafkaweb.repository.NotificationRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = KafkaWebApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class itNotificationRestControllerV1Test {

    private static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("testdb")
                    .withUsername("testuser")
                    .withPassword("testpass");

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("apache/kafka");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://" + POSTGRES_CONTAINER.getHost() + ":" + POSTGRES_CONTAINER.getMappedPort(5432) + "/" + POSTGRES_CONTAINER.getDatabaseName());
        registry.add("spring.r2dbc.username", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES_CONTAINER::getPassword);

        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);

        // Настройка подключения к базе данных
        registry.add("spring.datasource.url", POSTGRES_CONTAINER::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.datasource.password", POSTGRES_CONTAINER::getPassword);

        // Настройка Flyway
        registry.add("spring.flyway.url", POSTGRES_CONTAINER::getJdbcUrl);
        registry.add("spring.flyway.user", POSTGRES_CONTAINER::getUsername);
        registry.add("spring.flyway.password", POSTGRES_CONTAINER::getPassword);

        // Дополнительные параметры Flyway
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("spring.flyway.baseline-on-migrate", () -> "true");
        registry.add("spring.flyway.validate-on-migrate", () -> "true");
    }

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private NotificationRepository notificationRepository;

    @BeforeAll
    static void startContainer() {
        Startables.deepStart(POSTGRES_CONTAINER, KAFKA_CONTAINER).join();
    }

    @AfterAll
    static void stopContainer() {
        POSTGRES_CONTAINER.stop();
        KAFKA_CONTAINER.stop();
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        notificationRepository.deleteAll().block();
        produceMessagesToKafka();
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.findAll().collectList().block()).hasSize(3)
        );
    }

    @AfterEach
    void tearDown() {
        notificationRepository.deleteAll().block(); // Очищаем данные после каждого теста
    }

    private void produceMessagesToKafka() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Используем Set для проверки уникальности комбинации
            Set<String> uniqueKeys = new HashSet<>();

            int i = 0;
            while (uniqueKeys.size() < 3) {
                String userUid = UUID.randomUUID().toString();
                String objectId = UUID.randomUUID().toString();
                String triggerCode = (i % 2 == 0) ? "USER_REGISTRATION_1" : "DELETE_USER_2";

                String uniqueKey = objectId + ":" + "NEW" + ":" + "INTERNAL" + ":" + triggerCode;
                if (!uniqueKeys.add(uniqueKey)) {
                    continue; // Пропускаем повторяющиеся комбинации
                }

                String message = "{" +
                        "\"message\": \"User registered successfully\"," +
                        "\"messageType\": \"INTERNAL\"," +
                        "\"error\": null," +
                        "\"userUid\": \"" + userUid + "\"," +
                        "\"notificationStatus\": \"NEW\"," +
                        "\"triggerCode\": \"" + triggerCode + "\"," +
                        "\"objectType\": \"USER\"," +
                        "\"objectId\": \"" + objectId + "\"," +
                        "\"subject\": \"User Registration\"," +
                        "\"createdBy\": \"SYSTEM\"," +
                        "\"hasConfirmOtp\": true" +
                        "}";

                producer.send(new ProducerRecord<>("notifications", userUid, message)).get();
                i++;
            }
        }
    }

    @Test
    void getAllNotifications_success() {
        webTestClient.get()
                .uri("/api/v1/notifications")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(NotificationEntity.class)
                .hasSize(3)
                .value(notifications -> assertThat(notifications).isNotEmpty());
    }

    @Test
    void getAllNotifications_failure() {
        // Убедитесь, что база данных пуста
        notificationRepository.deleteAll().block();

        // Выполните запрос к контроллеру
        webTestClient.get()
                .uri("/api/v1/notifications")
                .exchange()
                .expectStatus().isOk() // Ожидаем статус 200 OK
                .expectBodyList(NotificationEntity.class)
                .hasSize(0) // Ожидаем, что список будет пустым
                .value(notifications -> assertThat(notifications).isEmpty());
    }

    @Test
    void getNotificationById_success() {
        // Ждём, пока сообщения из Kafka обработаются и сохранятся
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.findAll().collectList().block()).hasSize(3)
        );

        // Получаем одно из сохранённых уведомлений
        NotificationEntity savedNotification = notificationRepository.findAll().blockFirst();

        // Выполняем запрос к контроллеру по ID сохранённого уведомления
        assert savedNotification != null;
        webTestClient.get()
                .uri("/api/v1/notifications/" + savedNotification.getId())
                .exchange()
                .expectStatus().isOk() // Ожидаем статус 200 OK
                .expectBody(NotificationEntity.class)
                .value(response -> {
                    // Проверяем, что идентификатор совпадает
                    assertThat(response.getId()).isEqualTo(savedNotification.getId());
                    // Проверяем, что остальные данные корректны
                    assertThat(response.getMessage()).isEqualTo(savedNotification.getMessage());
                    assertThat(response.getNotificationStatus()).isEqualTo(savedNotification.getNotificationStatus());
                });
    }

    @Test
    void getNotificationById_failure() {
        // Убеждаемся, что база данных содержит сообщения из Kafka
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.findAll().collectList().block()).hasSize(3)
        );

        // Выполняем запрос к контроллеру с несуществующим идентификатором
        webTestClient.get()
                .uri("/api/v1/notifications/99999") // Несуществующий ID
                .exchange()
                .expectStatus().isNotFound(); // Ожидаем статус 404 Not Found
    }

    @Test
    void updateStatus_success() {
        // Убеждаемся, что сообщения из Kafka уже сохранены
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.findAll().collectList().block()).hasSize(3)
        );

        // Получаем одно из существующих уведомлений
        NotificationEntity notification = notificationRepository.findAll().blockFirst();
        assertThat(notification).isNotNull();

        // Выполняем запрос на обновление статуса
        webTestClient.patch()
                .uri("/api/v1/notifications/" + notification.getId() + "/status?status=COMPLETE")
                .exchange()
                .expectStatus().isNoContent(); // Успешное выполнение (204 No Content)

        // Проверяем, что статус обновился
        NotificationEntity updatedNotification = notificationRepository.findById(notification.getId()).block();
        assertThat(updatedNotification).isNotNull();
        assertThat(updatedNotification.getNotificationStatus()).isEqualTo(NotificationStatus.COMPLETE);
    }

    @Test
    void updateStatus_failure() {
        // Убеждаемся, что сообщения из Kafka уже сохранены
        await().atMost(10, SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.findAll().collectList().block()).hasSize(3)
        );

        // Выполняем запрос на обновление статуса с несуществующим ID
        webTestClient.patch()
                .uri("/api/v1/notifications/99999/status?status=COMPLETE") // Несуществующий ID
                .exchange()
                .expectStatus().isNotFound(); // Ожидаем статус 404 Not Found

        // Выполняем запрос с некорректным статусом
        NotificationEntity notification = notificationRepository.findAll().blockFirst();
        assertThat(notification).isNotNull();

        webTestClient.patch()
                .uri("/api/v1/notifications/" + notification.getId() + "/status?status=INVALID_STATUS")
                .exchange()
                .expectStatus().isBadRequest(); // Ожидаем статус 400 Bad Request
    }
}
