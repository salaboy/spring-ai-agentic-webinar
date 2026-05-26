package com.example.store.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;

import org.springframework.boot.restclient.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.support.ContextPropagatingTaskDecorator;

@Configuration(proxyBeanMethods = false)
public class ContextPropagationConfiguration {

    @Bean
    ContextPropagatingTaskDecorator contextPropagatingTaskDecorator() {
        return new ContextPropagatingTaskDecorator();
    }

    @Bean
    RestClientCustomizer tracePropagationRestClientCustomizer(OpenTelemetry openTelemetry) {
        System.err.println("====================================================");
        System.err.println("Creating RestClientCustomizer bean for tracing context propagation");
        System.err.println("====================================================");
        return builder -> builder.requestInterceptor((request, body, execution) -> {
            System.err.println("====================================================");
            System.err.println("Injecting tracing context into outgoing REST request");
            System.err.println("====================================================");
            openTelemetry.getPropagators().getTextMapPropagator().inject(
                    Context.current(),
                    request.getHeaders(),
                    (headers, key, value) -> headers.set(key, value)
            );
            return execution.execute(request, body);
        });
    }
}
