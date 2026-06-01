package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    private LibraryEventService libraryEventService;


    @KafkaListener(topics = {"library-events"},
    groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        try {
            log.info("Received: {}", consumerRecord.value());

            libraryEventService.processLibraryEvent(consumerRecord);

        } catch (Exception e) {
            log.error("🔥 ERROR WHILE PROCESSING MESSAGE", e);
        }
    }

}
