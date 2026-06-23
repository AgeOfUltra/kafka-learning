package com.microserviceproducer.ws.microserviceproducer.model;

import java.math.BigDecimal;

public record CreateProductRestModel(String title, BigDecimal price, int quantity){}
