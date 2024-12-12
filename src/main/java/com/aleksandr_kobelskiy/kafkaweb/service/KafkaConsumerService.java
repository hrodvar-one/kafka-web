//package com.aleksandr_kobelskiy.kafkaweb.service;
//
//import lombok.RequiredArgsConstructor;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//@Service
//@RequiredArgsConstructor
//public class KafkaConsumerService {
//
//    private final NotificationService notificationService;
//
//    @KafkaListener(topics = "notifications", groupId = "notifications-group")
//    public void consume(String message) {
//        notificationService.processMessage(message);
//    }
//}
