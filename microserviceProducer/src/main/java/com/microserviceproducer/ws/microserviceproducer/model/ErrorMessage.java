package com.microserviceproducer.ws.microserviceproducer.model;

import java.util.Date;

public record ErrorMessage(Date timeStamp, String message, String details) {
}
