package com.ravi.spring.batch.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ravi.spring.batch.entity.Product;
import com.ravi.spring.batch.repository.ProductRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class ProductServiceV2 {
    private final ProductRepository productRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topicName;

    public ProductServiceV2(ProductRepository productRepository, KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, @Value("${product.discount.update.topic}") String topicName) {
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
    }

    public void executeProductIds(List<Long> productIds) {
        List<List<Long>> batches = fetchBatches(productIds, 50);
        List<CompletableFuture<Void>> futures = batches.stream()
                .map(batch -> CompletableFuture.runAsync(() -> processProductIds(batch)))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public void processProductIds(List<Long> productIds) {
        System.out.println("Processing batch " + productIds + " by thread " + Thread.currentThread().getName());
        productIds.forEach(this::fetchUpdateAndPublish);
    }

    private void fetchUpdateAndPublish(Long id) {
        Product product = productRepository.findById(id).orElseThrow(() -> new IllegalArgumentException("Product id does not exist in DB"));
        updateDiscountedPrice(product);
        productRepository.save(product);
        publishEventToKafka(product);
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

    private void publishEventToKafka(Product product) {
        try {
            String productJson = objectMapper.writeValueAsString(product);
            kafkaTemplate.send(topicName, productJson);
        } catch (JsonProcessingException jpe) {
            throw new RuntimeException("Failed to convert product to json");
        }
    }

    private List<List<Long>> fetchBatches(List<Long> productIds, int batchSize) {
        List<List<Long>> batches = new ArrayList<>();
        int totalSize = productIds.size();
        int batchCount = (totalSize + batchSize - 1) / batchSize;
        for (int i = 0; i < batchCount; i++) {
            int start = i * batchSize;
            int end = Math.min(totalSize, (i + 1) * batchSize);
            batches.add(productIds.subList(start, end));
        }
        return batches;
    }
}
