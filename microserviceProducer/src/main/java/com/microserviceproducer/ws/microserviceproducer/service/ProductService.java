package com.microserviceproducer.ws.microserviceproducer.service;

import com.microserviceproducer.ws.microserviceproducer.model.CreateProductRestModel;

public interface ProductService {

    String createproduct(CreateProductRestModel productRestModel) throws Exception;

}
