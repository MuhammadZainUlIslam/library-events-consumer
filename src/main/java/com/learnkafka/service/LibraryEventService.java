package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private LibraryEventRepository libraryEventRepository;


    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {


        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(),LibraryEvent.class);

        log.info("Library Event : {}",libraryEvent);

        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;

            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type : {}",libraryEvent.getLibraryEventType());

        }



    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("Library Event Id is Missing");
        }

       Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());

        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Library Event Not Found");
        }
        log.info("Validation is successful for library event  : {}",libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {

        // FORCE NEW ENTITY
        libraryEvent.setLibraryEventId(null);

        if (libraryEvent.getBook() != null) {
            libraryEvent.getBook().setLibraryEvent(libraryEvent);
            libraryEvent.getBook().setBookId(null);
        }

        libraryEventRepository.save(libraryEvent);
    }
}
