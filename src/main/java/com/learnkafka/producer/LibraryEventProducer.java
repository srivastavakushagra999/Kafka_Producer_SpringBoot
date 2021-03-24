package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class acts as a kafka producer and publish the message in the kafka topic
 */
@Component
@Slf4j
public class LibraryEventProducer {
    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;
    String topic = "library-events";

    /**
     * This class publish the data in a Async fashion to the kafka topic also uses send default method
     * @param libraryEvent POJO which will be published to kafka topic
     * @throws JsonProcessingException
     */
    public void sendAsyncDefaultLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
      ListenableFuture<SendResult<Integer,String>> listenableFuture= kafkaTemplate.sendDefault(key,value);
      listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
                                       @Override
                                       public void onFailure(Throwable ex) {

                                           handleError(key,value,ex);
                                       }

                                       @Override
                                       public void onSuccess(SendResult<Integer, String> result) {
                                           handleSuccess(key,value,result);

                                       }
                                   }

      );

    }

    /**
     * Sends the data in the sync fashion
     * @param libraryEvent POJO which will be published to kafka topic
     * @throws JsonProcessingException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void sendSyncLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult=null;
        try {
            sendResult = kafkaTemplate.send(topic, key, value).get(2, TimeUnit.SECONDS);
        }
        catch (ExecutionException| InterruptedException e)
        {
            log.error("ExecutionException| InterruptedException occurred while writing the message {}, Exception message {}",
                    sendResult.getProducerRecord().value(),e.getMessage());
        }
        catch (Exception e)
        {
            log.error("Exception occurred while writing the message {}, Exception message {}",
                    sendResult.getProducerRecord().value(),e.getMessage());
        }
    }

    /**
     * Publish the POJO in Async fashion also publishes the message in form of producer record and also adds the header in the message
     * @param libraryEvent
     * @throws JsonProcessingException
     */

    public void sendAsyncProducerEventLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        List<Header> recordHeaders = new ArrayList<Header>();
               recordHeaders.add(new RecordHeader("library-event", value.getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<Integer,String> producerRecord = new ProducerRecord<>(topic,null,key,value,recordHeaders);
        ListenableFuture<SendResult<Integer,String>> listenableFuture= kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
                                         @Override
                                         public void onFailure(Throwable ex) {

                                             handleError(key,value,ex);
                                         }

                                         @Override
                                         public void onSuccess(SendResult<Integer, String> result) {
                                             handleSuccess(key,value,result);

                                         }
                                     }

        );

    }

    private void handleError(Integer key, String value, Throwable ex) {
        log.error("Message got failed to publish : Key: {},value:{},Exception:{}",key,value,ex.getMessage());
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message successfully got published key: {},value:{},partition: {} "
                ,key,value,result.getRecordMetadata().partition());
    }
}
