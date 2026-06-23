package com.emailnotificationms.ws.emailnotification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class EmailNotificationMicroServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(EmailNotificationMicroServiceApplication.class, args);
    }

    @Bean
    RestTemplate restTemplate(){
        return  new RestTemplate();
    }

}
