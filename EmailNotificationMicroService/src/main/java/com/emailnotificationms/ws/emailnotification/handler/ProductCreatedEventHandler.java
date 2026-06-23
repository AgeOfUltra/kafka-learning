package com.emailnotificationms.ws.emailnotification.handler;

import com.emailnotificationms.ws.emailnotification.error.NonRetryableException;
import com.emailnotificationms.ws.emailnotification.error.RetryableException;
import com.microservicecore.ws.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic") //groupId = "product-created-events"
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    RestTemplate restTemplate;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){

//        if(true) throw new NonRetryableException("An error took place no, need to consume the message");

        LOGGER.info("Received a new event "+productCreatedEvent.getTitle()+" with "+productCreatedEvent.getProductId());

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


    }
}
