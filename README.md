# Реактивное веб-приложение "kafka-web"

## Оглавление
[1. Описание проекта](#описание-проекта)<br>
[2. Запуск проекта](#запуск-проекта)  
[3. Отправка начальных сообщений в KAFKA через консоль](#отправка-начальных-сообщений-в-kafka-через-консоль)<br>
[4. Виды запросов к API через Postman](#виды-запросов-к-api-через-postman)<br>
&nbsp;&nbsp;&nbsp;&nbsp;[- Notifications_Collection](#notifications_collection)<br>
[5. UML диаграммы](#uml-диаграммы)

### Описание проекта

Реактивное веб-приложение "kafka-web" обрабатывает Сообщения через Kafka:<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Приложение принимает сообщения из Apache Kafka.<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Каждое сообщение содержит JSON для объекта notification.<br>

Хранение данных:<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Система сохраняет полученные сообщения в базе данных Postgresql в таблице notifications.<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Миграция БД выполняется средствами Flyway.<br>

Реализация API:<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - реализовано API для получения всех нотификаций с пагинацией.<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - реализован функционал для получения нотификаций по id.<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - реализована возможность изменения статуса нотификации с NEW на COMPLETE.<br>

Контейнеризация с Docker:<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Проект упакован в Docker-контейнеры, используя Docker Compose для оркестрации.<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Настроена Kafka и Postgres в Docker, обеспечивая их связь с основным приложением.<br>

Тестирование:<br>
&nbsp;&nbsp;&nbsp;&nbsp;    - Реализованы интеграционные тесты с использованием TestContainers для взаимодействия с Kafka и Postgres.<br>

Отправка сообщений в Kafka осуществляется через командную строку.<br>

### Запуск проекта

* Запустите Docker и проверьте что интернет соединение доступно.
* Для запуска приложения скачайте проект в нужную директорию:
  ```
  git clone https://gitlab.leantech.ai/javateamio/kafka-web.git
  ```
* Перейдите в директорию склонированного проекта и в командной строке введите команду:
  ```
  docker-compose up --build
  ```
* Начнётся процесс сборки и развёртывания Docker контейнера приложения.
  Этот процесс обычно длится несколько минут.
* Приложение будет запущено по адресу:
  ```
  http://localhost:8687/
  ```
* Дальнейшее взаимодействие осуществляется через Postman.

### Отправка начальных сообщений в KAFKA через консоль

1. Выполняем команду просмотра запущенных контейнеров в докере

  ```
  docker ps
  ```
2. Копируем CONTAINER ID контейнера Kafka из списка запущенных контейнеров

![docker_containers_list.png](/img/docker_containers_list.png)

3. Вставляем ID в строку вида и жмём Enter:

  ```
  docker exec -it <kafka-container-name> bash
  ```
  Пример:

![kafka_container_enter_command.png](/img/kafka_container_enter_command.png) 

4. Таким образом мы попадаем в консоль контейнера Kafka

![kafka_container_console.png](/img/kafka_container_console.png)

6. Теперь можно отправлять сообщения непосредственно через консоль (копируем команду, вставляем в консоль и жмём Enter):

  ```
  echo '{"message": "User registered successfully", "messageType": "INTERNAL", "error": null, "userUid": "123e4567-e89b-12d3-a456-426614174000", "notificationStatus": "NEW", "triggerCode": "USER_REGISTRATION_1", "objectType": "USER", "objectId": "123e4567-e89b-12d3-a456-426614174001", "subject": "User Registration", "createdBy": "SYSTEM", "hasConfirmOtp": true}' | kafka-console-producer --bootstrap-server localhost:9092 --topic notifications

  ```

  ```
  echo '{"message": "User registered successfully", "messageType": "EXTERNAL", "error": null, "userUid": "123e4567-e89b-12d3-a456-426614174000", "notificationStatus": "COMPLETE", "triggerCode": "DELETE_USER_2", "objectType": "USER", "objectId": "123e4567-e89b-12d3-a456-426614174002", "subject": "User Registration", "createdBy": "SYSTEM", "hasConfirmOtp": true}' | kafka-console-producer --bootstrap-server localhost:9092 --topic notifications

  
  ```
  
  ```
  echo '{"message": "User registered successfully", "messageType": "INTERNAL", "error": null, "userUid": "123e4567-e89b-12d3-a456-426614174000", "notificationStatus": "NEW", "triggerCode": "USER_REGISTRATION_1", "objectType": "USER", "objectId": "123e4567-e89b-12d3-a456-426614174003", "subject": "User Registration", "createdBy": "SYSTEM", "hasConfirmOtp": true}' | kafka-console-producer --bootstrap-server localhost:9092 --topic notifications


  ```

  ```
  echo '{"message": "User registered successfully", "messageType": "INTERNAL", "error": null, "userUid": "123e4567-e89b-12d3-a456-426614174000", "notificationStatus": "NEW", "triggerCode": "USER_REGISTRATION_1", "objectType": "USER", "objectId": "123e4567-e89b-12d3-a456-426614174004", "subject": "User Registration", "createdBy": "SYSTEM", "hasConfirmOtp": true}' | kafka-console-producer --bootstrap-server localhost:9092 --topic notifications

  ```

7. Таким образом мы передали 4 сообщения в Kafka и эти сообщения автоматически забрало приложение и записало в базу Postgres.


### Виды запросов к API через Postman

Примеры запросов:

### Notifications_Collection

<div style="color: white;
            font-weight: bold;
            text-align-last: center;
            background-color: cornflowerblue;
            width: 90px; padding: 10px;
            border-radius: 5px;
            text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.2);">
    GET
</div><br> 
(GET_ALL_NOTIFICATIONS)

  ```
  http://localhost:8687/api/v1/notifications
  ```
данный запрос вернёт JSON который содержит все notification которые есть в базе, либо вернёт []:
  ```
  [
    {
        "id": 1,
        "createdAt": "2024-12-13T17:40:20.373554",
        "modifiedAt": null,
        "expirationDate": "2024-12-20T17:40:20.295579",
        "message": "User registered successfully",
        "messageType": "INTERNAL",
        "error": null,
        "userUid": "123e4567-e89b-12d3-a456-426614174000",
        "notificationStatus": "NEW",
        "triggerCode": "USER_REGISTRATION_1",
        "objectType": "USER",
        "objectId": "123e4567-e89b-12d3-a456-426614174001",
        "subject": "User Registration",
        "createdBy": "SYSTEM",
        "hasConfirmOtp": true
    },
    {
        "id": 2,
        "createdAt": "2024-12-13T17:42:07.475165",
        "modifiedAt": null,
        "expirationDate": "2024-12-20T17:42:07.471978",
        "message": "User registered successfully",
        "messageType": "EXTERNAL",
        "error": null,
        "userUid": "123e4567-e89b-12d3-a456-426614174000",
        "notificationStatus": "COMPLETE",
        "triggerCode": "DELETE_USER_2",
        "objectType": "USER",
        "objectId": "123e4567-e89b-12d3-a456-426614174002",
        "subject": "User Registration",
        "createdBy": "SYSTEM",
        "hasConfirmOtp": true
    }
]
  ```

<div style="color: white;
            font-weight: bold;
            text-align-last: center;
            background-color: cornflowerblue;
            width: 90px; padding: 10px;
            border-radius: 5px;
            text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.2);">
    GET
</div><br> 
(GET_NOTIFICATION_BY_ID)

  ```
  http://localhost:8687/api/v1/notifications/1
  ```
в конец строки запроса нужно подставить id интересующего нас notification и тогда при выполнении вернёт JSON к конкретным notification:
  ```
  {
    "id": 1,
    "createdAt": "2024-12-13T17:40:20.373554",
    "modifiedAt": "2024-12-13T17:42:28.592122",
    "expirationDate": "2024-12-20T17:40:20.295579",
    "message": "User registered successfully",
    "messageType": "INTERNAL",
    "error": null,
    "userUid": "123e4567-e89b-12d3-a456-426614174000",
    "notificationStatus": "COMPLETE",
    "triggerCode": "USER_REGISTRATION_1",
    "objectType": "USER",
    "objectId": "123e4567-e89b-12d3-a456-426614174001",
    "subject": "User Registration",
    "createdBy": "SYSTEM",
    "hasConfirmOtp": true
  }
  ```

Если notification с id переданным в запросе не найден, то вернёт 404 Not Found.

<div style="color: white;
            font-weight: bold;
            text-align-last: center;
            background-color: #97687b;
            width: 90px; padding: 10px;
            border-radius: 5px;
            text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.2);">
    PATCH
</div><br> 
(PATCH_NOTIFICATION_STATUS)

  ```
  localhost:8687/api/v1/notifications/1/status?status=COMPLETE
  ```
Данный запрос поменяет статус на COMPLETE у notification с id 1, иначе если notification не найден, то вернёт 404 Not Found.

***

### UML диаграммы

![UML_class_diagram.svg](/img/UML_class_diagram.svg)<br>
![UML_er_diagram.png](/img/ER_diagram.png)<br>
![UML_sequence_diagram.png](/img/UML_sequence_diagram.png)<br>
![UML_component_diagram.png](/img/UML_component_diagram.png)<br>