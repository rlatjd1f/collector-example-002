package com.example.collectorexample002;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CollectorExample002Application {

    public static void main(String[] args) {
        SpringApplication.run(CollectorExample002Application.class, args);
    }

}
