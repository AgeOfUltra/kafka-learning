package com.microserviceproducer.ws.microserviceproducer.controller;

import com.microserviceproducer.ws.microserviceproducer.model.CreateProductRestModel;
import com.microserviceproducer.ws.microserviceproducer.model.ErrorMessage;
import com.microserviceproducer.ws.microserviceproducer.service.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.Objects;

@RestController
@RequestMapping("/products")
public class ProductController {

    ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    public ResponseEntity<Object> createProduct(@RequestBody CreateProductRestModel prodct){
        String productId;
        try {
            productId = productService.createproduct(prodct);
        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorMessage(new Date(), e.getMessage(), "/products"));
        }
        return  ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
