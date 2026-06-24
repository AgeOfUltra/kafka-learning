package com.emailnotificationms.ws.emailnotification.handler;

import com.emailnotificationms.ws.emailnotification.entity.ProcessedEventEntity;
import com.emailnotificationms.ws.emailnotification.error.NonRetryableException;
import com.emailnotificationms.ws.emailnotification.error.RetryableException;
import com.emailnotificationms.ws.emailnotification.repo.ProcessedEventRepository;
import com.microservicecore.ws.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic") //groupId = "product-created-events"
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    RestTemplate restTemplate;

    private ProcessedEventRepository processedEventRepo;

    public ProductCreatedEventHandler(RestTemplate restTemplate,ProcessedEventRepository repo ) {
        this.restTemplate = restTemplate;
        this.processedEventRepo=repo;
    }

    @KafkaHandler
    @Transactional
    //  here  in the method argument we are binding Payload and the header to respective fields
//    to make the feilds option we can make also add the another param as required = false inside the @Header or @Payload
    public void handle(@Payload ProductCreatedEvent productCreatedEvent, @Header("messageId") String messageId, @Header(KafkaHeaders.RECEIVED_KEY) String messagekey){

//        if(true) throw new NonRetryableException("An error took place no, need to consume the message");

        LOGGER.info("Received a new event "+productCreatedEvent.getTitle()+" with "+productCreatedEvent.getProductId());


        //check if the message was already processed
        ProcessedEventEntity existingRecord =processedEventRepo.findByMessageId(messageId);

        if(existingRecord!=null){
            LOGGER.info("Found Duplicate MessageId "+existingRecord.getMessageId());
            return;
        }

        String theurl = "http://localhost:8082/response/200";

        ResponseEntity<String> response = null;
        try {
            response = restTemplate.exchange(theurl, HttpMethod.GET,null,String.class);
            if(response.getStatusCode().value()== HttpStatus.OK.value()){
                LOGGER.info("received Response body from remote service "+response.getBody());
            }
        } catch (ResourceAccessException e) {
            LOGGER.error(e.getMessage());

            throw new RetryableException(e);
        }
        catch (HttpServerErrorException ex){
            LOGGER.error(ex.getMessage());
            throw new NonRetryableException(ex);
        }
        catch (Exception ex){
            LOGGER.error(ex.getMessage());
            throw new NonRetryableException(ex);
        }


        try{

            // save  unique message id in a database table.
            processedEventRepo.save(new ProcessedEventEntity(messageId,productCreatedEvent.getProductId()));

        }catch (DataIntegrityViolationException e){
            throw new NonRetryableException(e);
        }
    }
}
