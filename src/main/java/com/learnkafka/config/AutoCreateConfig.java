package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;


/**
 * This method automatically creates the topic in the kafka
 */
@Configuration
@Profile("local")
public class AutoCreateConfig {
    @Bean
    public NewTopic libraryTopicCreation()
    {
        return TopicBuilder.name("library-events")
                .partitions(3)
                .replicas(1)
                .build();
    }
}