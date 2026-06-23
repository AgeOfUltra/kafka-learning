package com.microserviceproducer.ws.microserviceproducer.service;

import com.microservicecore.ws.core.ProductCreatedEvent;
import com.microserviceproducer.ws.microserviceproducer.model.CreateProductRestModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Service
public class ProductServiceImpl implements  ProductService{

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


//    Asynchronous
//    @Override
//    public String createproduct(CreateProductRestModel productRestModel) {
//        String productId = UUID.randomUUID().toString();
//
////        TODO : Persist product Details into database table before publishing an event.
//
//        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
//                productRestModel.title(),productRestModel.price(),productRestModel.quantity());
//
//
//    // sends asynchronous requests with no successful response.
//    // but if we add the Completable future it will wait for the successful response.
//
//        CompletableFuture<SendResult<String,ProductCreatedEvent>> future =  kafkaTemplate.send("product-created-events-topic", productId,productCreatedEvent);
//
//        future.whenComplete((result,exception) -> {
//            if(exception!=null){
//                LOGGER.error("failed to send Message "+exception.getMessage());
//            }else{
//                LOGGER.info("message sent successfully "+ result.getRecordMetadata());
//            }
//        });
//
////        future.join(); // by adding this main thread will wait for the async thread to complete it ( ideal it will act as sync request)
//        LOGGER.info("******* returning product info");
//        return productId;
//    }


//synchronous
    @Override
    public String createproduct(CreateProductRestModel productRestModel) throws Exception {
        String productId = UUID.randomUUID().toString();

//        TODO : Persist product Details into database table before publishing an event.

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
                productRestModel.title(),productRestModel.price(),productRestModel.quantity());


        LOGGER.info("Before publishing a productCreated event");

        // sends asynchronous requests with no successful response.
        // but if we add the Completable future it will wait for the successful response.

        SendResult<String,ProductCreatedEvent> result =
                kafkaTemplate.send("product-created-events-topic", productId,productCreatedEvent).get();
        LOGGER.info("Partition data "+result.getRecordMetadata().partition());
        LOGGER.info("Partition data "+result.getRecordMetadata().topic());
        LOGGER.info("OffSet data "+result.getRecordMetadata().offset());

        LOGGER.info("******* returning product info");
        return productId;
    }
}
