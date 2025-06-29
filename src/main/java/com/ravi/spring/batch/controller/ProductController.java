package com.ravi.spring.batch.controller;

import com.ravi.spring.batch.service.ProductService;
import com.ravi.spring.batch.service.ProductServiceV2;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/products")
public class ProductController {
    private final ProductService productService;
    private final ProductServiceV2 productServiceV2;

    public ProductController(ProductService productService, ProductServiceV2 productServiceV2) {
        this.productService = productService;
        this.productServiceV2 = productServiceV2;
    }

    @GetMapping("/ids")
    public ResponseEntity<List<Long>> getIds() {
        return ResponseEntity.ok(productService.getProductIds());
    }

    @PostMapping("/reset")
    public ResponseEntity<String> resetProducts() {
        return ResponseEntity.ok(productService.resetRecords());
    }

    @PostMapping("/process")
    public ResponseEntity<String> processProductIds(@RequestBody List<Long> ids) {
        productService.processProductIds(ids);
        return ResponseEntity.ok("Products processed and events published to kafka");
    }

    @PostMapping("/process/v2")
    public ResponseEntity<String> processProductIdsV2(@RequestBody List<Long> ids) {
        productServiceV2.executeProductIds(ids);
        return ResponseEntity.ok("Products processed and events published to kafka");
    }
}
