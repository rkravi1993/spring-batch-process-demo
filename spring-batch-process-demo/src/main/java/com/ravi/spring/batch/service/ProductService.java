package com.ravi.spring.batch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ravi.spring.batch.entity.Product;
import com.ravi.spring.batch.repository.ProductRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ProductService {
    private final ProductRepository productRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topicName;

    public ProductService(ProductRepository productRepository, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, @Value("${product.discount.update.topic}") String topicName) {
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
    }

    public String resetRecords() {
        productRepository.findAll().forEach(product -> {
            product.setOfferApplied(false);
            product.setDiscountPercentage(0);
            product.setPriceAfterDiscount(product.getPrice());
            productRepository.save(product);
        });
        return "Data reset done in DB";
    }

    public void processProductIds(List<Long> productIds) {
        productIds.forEach(this::fetchUpdateAndPublish);
    }

    private void fetchUpdateAndPublish(Long id) {
        Product product = productRepository.findById(id).orElseThrow(() -> new IllegalArgumentException("Product id does not exist in DB"));
        updateDiscountedPrice(product);
        productRepository.save(product);
        publishEventToKafka(product);
    }

    private void publishEventToKafka(Product product) {
        try {
            String productJson = objectMapper.writeValueAsString(product);
            kafkaTemplate.send(topicName, productJson);
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException("Failed to convert product to json");
        }
    }

    private void updateDiscountedPrice(Product product) {
        double price = product.getPrice();
        int discountedPrice = (price >= 1000) ? 10 : (price > 500) ? 5 : 0;
        double priceAfterDiscount = price - (price * discountedPrice / 100);
        if (discountedPrice > 0) {
            product.setOfferApplied(true);
        }
        product.setDiscountPercentage(discountedPrice);
        product.setPriceAfterDiscount(priceAfterDiscount);
    }

    public List<Long> getProductIds() {
        return productRepository.findAll().stream().map(Product::getId).collect(Collectors.toList());
    }
}
