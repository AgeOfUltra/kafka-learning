package com.emailnotificationms.ws.emailnotification.config;

import com.emailnotificationms.ws.emailnotification.error.NonRetryableException;
import com.emailnotificationms.ws.emailnotification.error.RetryableException;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;


import java.util.HashMap;
import java.util.Map;

@Configuration
public class kafkaConsumerConfig {


//    @Value("${spring.kafka.consumer.bootstrap-servers}")
//    private String servers;

    @Autowired
    Environment environment;

    @Bean
    ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class); // it will deserialize the value

//        this below line says that value deserializer class to error handling deserializer which will catch any deserialization exceptions
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,JacksonJsonDeserializer.class); // it says that while error handling the serializer class is used as value deserializer
//        then it will use this Json Serilaizer class to deserialize message

        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES,
                environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.package"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory, KafkaTemplate<String, Object> kafkaTemplate) {

         // it provides error handling error capability for kafka consumers, and it is used to handle expectation that occurred while message consumption by kafka listiner
//now failed message will be sent to dead letter topic
//        kafka template is used to send the error message to dead letter topic , becoz kafka template wraps producer methods and snd the message to topics

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(5000,3));
       // by adding FixedbackOff stragetdy it add new behaviour to the defaulthandler while handling Retryable exception

        errorHandler.addNotRetryableExceptions(NonRetryableException.class); // this is used to handle only non retryable Exception only
//        we can add multiple Exceptions with , seperated.
//        HttpServerErrorException -> becoz it will be used handle the exception when our microservice is trying to connect with other microservice using http and it sends 500 code error
//  instead of handling it as NonRetrableException we can tell it has HttpServerError

        errorHandler.addRetryableExceptions(RetryableException.class); // to handle all the retryable exception


        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler); //if error occures during message consumption by kafka listiner,
//        the default error handler instance    will be used to handle the instance, when exceptiin stakes place the defalt error handles it will rewind the the partition after message failure so that it can replay.

        return factory;
    }

    @Bean
    KafkaTemplate<String,Object> kafkaTemplate( ProducerFactory<String,Object> producerFactory){
        return  new KafkaTemplate<>(producerFactory);
    }



    @Bean
    ProducerFactory<String,Object> producerFactory(){
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,environment.getProperty("spring.kafka.consumer.bootstrap-servers")); // to establish connection between kafka clusters.
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class); // telling kafka consumer to use json serilizer and convert message value into jsonFormat before sending it to kafka
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);

        return  new DefaultKafkaProducerFactory<>(config);
    }
}


/*
*
* DefaultErrorHandler we can configure the Error handler to handle this error object with list of exceptions that are not retryable
*
* so that if a notretryable exception is thrown this error handler will not attmpt to retry consuming it agian and it will send the error message to dead letter topc again
* */
