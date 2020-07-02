package com.itians.learn.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itians.learn.kafka.entity.Book;
import com.itians.learn.kafka.entity.LibraryEvent;
import com.itians.learn.kafka.entity.LibraryEventType;
import com.itians.learn.kafka.repository.LibraryEventsRepository;
import com.itians.learn.kafka.service.LibraryEventsService;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers = ${spring.embedded.kafka.brokers}"})
class LibraryEventsConsumerTestIT {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    // SpyBean is annotation which will give you access to the real bean
    @SpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    private LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    //KafkaListenerEndpointRegistry hold all the listener containers like LibraryEventConsumer
    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        //to make the consumer completely up and running before launch test case
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            // make sure that the container we have which is LibraryEventConsumer is going to wait until all partitions
            // are assigned
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka with Spring Boot\",\"bookAuthor\":\"Ibrahim Mohsen\"}}";
        kafkaTemplate.sendDefault(json).get();
        //when
        //CountDownLatch help us block the current execution of the thread as the call will be asynchronous
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertEquals(1, libraryEventList.size());
        libraryEventList.forEach(libraryEvent -> {
            assertNotEquals(null, libraryEvent.getLibraryEventId());
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        //1- first save LibraryEvent Obj to DB Then try to update it
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka with Spring Boot\",\"bookAuthor\":\"Ibrahim Mohsen\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        //2- publish the update library event
        Book updatedBook = Book.builder()
                .bookId(123).bookName("Kafka with Spring Boot 2.x").bookAuthor("Ibrahim Mohsen").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        //when
        //CountDownLatch help us block the current execution of the thread as the call will be asynchronous
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        //then
        //verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        //verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent updatedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka with Spring Boot 2.x", updatedLibraryEvent.getBook().getBookName());

    }

    @Test
    void publishUpdateLibraryEvent_With_Not_A_Valid_LibraryEventId() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        Integer libraryEventId = 789;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka with Spring Boot\",\"bookAuthor\":\"Ibrahim Mohsen\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        //CountDownLatch help us block the current execution of the thread as the call will be asynchronous
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());

    }

    @Test
    void publishModifyLibraryEvent_With_Null_LibraryEventId() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka with Spring Boot\",\"bookAuthor\":\"Ibrahim Mohsen\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        //CountDownLatch help us block the current execution of the thread as the call will be asynchronous
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    }

    @Test
    void publishModifyLibraryEvent_With_000_LibraryEventId() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka with Spring Boot\",\"bookAuthor\":\"Ibrahim Mohsen\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        //CountDownLatch help us block the current execution of the thread as the call will be asynchronous
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        //then
        //verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(7)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(2)).handleRecovery(isA(ConsumerRecord.class));

    }
}