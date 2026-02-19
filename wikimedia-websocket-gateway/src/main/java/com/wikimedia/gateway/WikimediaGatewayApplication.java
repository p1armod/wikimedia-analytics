package com.wikimedia.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class WikimediaGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(WikimediaGatewayApplication.class, args);
    }
}
