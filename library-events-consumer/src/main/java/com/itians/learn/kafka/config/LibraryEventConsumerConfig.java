package com.itians.learn.kafka.config;

import com.itians.learn.kafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {

    @Autowired
    private LibraryEventsService libraryEventsService;
    //add been to manually mange commit_offset
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, kafkaConsumerFactory);

        //to make your kafkaListenerContainer work in multiple threads and this option is useful
        // when you run your application in non cloud environment like Kubernetes
        factory.setConcurrency(3);

        //Default __commit_offset is BATCH
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        //custom error handler for LoggingErrorHandler in kafka
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
        }));

        //Retry in Kafka
        factory.setRetryTemplate(retryTemplate());

        //Recovery in Kafka : used when all retries attempts occurred but there still error and the record not processed
        factory.setRecoveryCallback((context -> {
            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                // Invoke Recovery logic
                log.info("Inside the recoverable logic");
                //display all attributes in context
                Arrays.asList(context.attributeNames())
                        .forEach(attributeName -> {
                            log.info("Attribute Name is : {}",attributeName);
                            log.info("Attribute Value is : {}",context.getAttribute(attributeName));
                });
                ConsumerRecord<Integer,String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute(
                        "record");

                libraryEventsService.handleRecovery(consumerRecord);

            } else {
                log.info("Inside the non recoverable logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        }));
        return factory;
    }

    private RetryTemplate retryTemplate() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000); // 1000 millisecond = 1 second => means try after 1 second

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        //SimpleRetryPolicy simpleRetryPolicy =  new SimpleRetryPolicy();
        //if any record fails then in that case it's going to retry for three times
        //simpleRetryPolicy.setMaxAttempts(3);

        //HashMap to put all exception types that we want and we don't want to retry
        Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
        exceptionsMap.put(IllegalArgumentException.class, false);
        exceptionsMap.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMap, true);
        return simpleRetryPolicy;
    }
}
