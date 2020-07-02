package com.itians.learn.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itians.learn.kafka.domain.Book;
import com.itians.learn.kafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerUT {

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(libraryEventProducer,"topicName","library-events");
    }

    @Test
    void sendLibraryEventApproach2_failure() throws JsonProcessingException, ExecutionException, InterruptedException  {
        //given
        Book book = Book.builder()
                .bookId(123)
                .bookName("Kafka with Spring Boot")
                .bookAuthor("Ibrahim Mohsen")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String record = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events",
                libraryEvent.getLibraryEventId(),record);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events",1),
                1,1,342,System.currentTimeMillis(),1,
                2);
        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord,recordMetadata);
        SettableListenableFuture future = new SettableListenableFuture();
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        //when
        ListenableFuture<SendResult<Integer, String>>  listenableFuture =libraryEventProducer.sendLibraryEventApproach2(libraryEvent);

        //then
        SendResult<Integer, String > sendResultFinal = listenableFuture.get();
        assertEquals(1,sendResultFinal.getRecordMetadata().partition());
    }

    @Test
    void sendLibraryEventApproach2_success() throws JsonProcessingException, ExecutionException, InterruptedException  {
        //given
        Book book = Book.builder()
                .bookId(123)
                .bookName("Kafka with Spring Boot")
                .bookAuthor("Ibrahim Mohsen")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
        //when
        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventApproach2(libraryEvent).get());
        //then
    }
}