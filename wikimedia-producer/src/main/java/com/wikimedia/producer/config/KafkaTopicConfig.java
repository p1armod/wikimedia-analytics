package com.wikimedia.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


@Configuration
public class KafkaTopicConfig {

    // Configure Spring Bean
    @Bean
    public NewTopic recentChangeTopic() {
        return TopicBuilder.name("wikimedia.recentchange")
                .partitions(6)
                .replicas(1)
                .build();
    }

    // Configure Spring Bean
    @Bean
    public NewTopic statsTopic() {
        return TopicBuilder.name("wikimedia.stats")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Configure Spring Bean
    @Bean
    public NewTopic topWikisTopic() {
        return TopicBuilder.name("wikimedia.top-wikis")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Configure Spring Bean
    @Bean
    public NewTopic botRatioTopic() {
        return TopicBuilder.name("wikimedia.bot-ratio")
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Configure Spring Bean
    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder.name("wikimedia.alerts")
                .partitions(1)
                .replicas(1)
                .build();
    }

    // Configure Spring Bean
    @Bean
    public NewTopic feedTopic() {
        return TopicBuilder.name("wikimedia.feed")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
