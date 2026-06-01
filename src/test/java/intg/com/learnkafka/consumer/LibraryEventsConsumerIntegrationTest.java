package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.repository.LibraryEventRepository;
import com.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import java.util.concurrent.CountDownLatch;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.ArgumentMatchers.any;
import java.util.concurrent.ExecutionException;


@SpringBootTest
@EmbeddedKafka(topics = "library-events")
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventRepository;


    @BeforeEach
    void setUp() {

        for(MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }

    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test

    /*given*/
    void publishLibraryEvents() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{"
                + "\"libraryEventId\": null,"
                + "\"libraryEventType\": \"NEW\","
                + "\"book\": {"
                + "\"bookId\": 1,"
                + "\"bookName\": \"Kafka Spring Book\","
                + "\"bookAuthor\": \"Zain\""
                + "}"
                + "}";
    kafkaTemplate.sendDefault(json).get();

    /*When*/
        CountDownLatch latch = new CountDownLatch(1);
        await()
                .atMost(5, SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, times(1))
                            .onMessage(any(ConsumerRecord.class));
                });

    /*Then*/
        verify(libraryEventsConsumerSpy, times(1))
                .onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1))
                .processLibraryEvent(isA(ConsumerRecord.class));

/*        List<LibraryEvent> libraryEventList= (List<LibraryEvent>) libraryEventRepository.findAll();
        assertEquals(1, libraryEventList.size());        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId()!=null;
            assertEquals(1,libraryEvent.getBook().getBookId());

        });*/
    }

}
