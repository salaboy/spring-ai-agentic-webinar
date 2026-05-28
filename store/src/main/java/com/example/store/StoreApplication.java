package com.example.store;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.micrometer.metrics.autoconfigure.export.otlp.OtlpMetricsExportAutoConfiguration;
import reactor.core.publisher.Hooks;

@SpringBootApplication(exclude = OtlpMetricsExportAutoConfiguration.class)
public class StoreApplication {

    public static void main(String[] args) {
        Hooks.enableAutomaticContextPropagation();
        SpringApplication.run(StoreApplication.class, args);
    }
}
