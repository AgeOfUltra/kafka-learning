package com.emailnotificationms.ws.emailnotification.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@Entity
@Table(name = "processed-events")
@Data
public class ProcessedEventEntity implements Serializable {
    @Id
    @GeneratedValue
    private Long id;

    @Column(nullable = false,unique = true)
    private String messageId;

    @Column(nullable = false)
    private String productId;


    public ProcessedEventEntity(String messageId, String productId) {
        this.messageId = messageId;
        this.productId = productId;
    }

    public ProcessedEventEntity() {

    }
}
