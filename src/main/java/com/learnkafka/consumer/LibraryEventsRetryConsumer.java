package com.learnkafka.consumer;

import com.learnkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsRetryConsumer {

    @Autowired
    private LibraryEventService libraryEventService;


    @KafkaListener(topics = {"${topics.retry}"}, groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        try {
            log.info("Consumer Record in Retry Consumer: {}", consumerRecord.value());

            libraryEventService.processLibraryEvent(consumerRecord);

        } catch (Exception e) {
            log.error("🔥 ERROR WHILE PROCESSING MESSAGE", e);
        }
    }

}
