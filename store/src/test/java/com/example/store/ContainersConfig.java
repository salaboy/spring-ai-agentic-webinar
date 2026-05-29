package com.example.store;

import io.github.microcks.testcontainers.MicrocksContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import io.arconia.opentelemetry.autoconfigure.traces.exporter.otlp.OtlpTracingConnectionDetails;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.Nullable;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class ContainersConfig {

    @Bean
    Network network() {
        return Network.newNetwork();
    }

    @Bean
    OtlpTracingConnectionDetails otlpTracingConnectionDetails() {
        return transport -> System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") + "/v1/traces";
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer(Network network) {
        return new KafkaContainer("apache/kafka")
                .withNetwork(network)
                .withNetworkAliases("kafka");
    }

    @Bean
    @ConditionalOnProperty(name = "microcks.enabled", havingValue = "true")
    MicrocksContainer microcks(Network network) {
        // Microcks official image.
        return new MicrocksContainer("quay.io/microcks/microcks-uber:nightly-native")
            .withNetwork(network)
            .withMainArtifacts("anthropic-openapi.yaml")
            .withSecondaryArtifacts("anthropic-metadata.yaml", "anthropic-examples.yaml")
            .withEnv("OTEL_EXPORTER_OTLP_ENDPOINT", System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .withEnv("OTEL_EXPORTER_OTLP_HEADERS_AUTHORIZATION", System.getenv("OTEL_EXPORTER_OTLP_HEADERS_AUTHORIZATION"))
            .withDebugLogLevel();

        /*
        // lbroudoux test image: works correctly. 
        return new MicrocksContainer(DockerImageName.parse("quay.io/lbroudoux/microcks-uber:nightly-native")
            .asCompatibleSubstituteFor("quay.io/microcks/microcks-uber:nightly-native"))
            .withNetwork(network)
            .withMainArtifacts("anthropic-openapi.yaml")
            .withSecondaryArtifacts("anthropic-metadata.yaml", "anthropic-examples.yaml")
            .withEnv("OTEL_EXPORTER_OTLP_ENDPOINT", System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .withEnv("OTEL_EXPORTER_OTLP_HEADERS_AUTHORIZATION", System.getenv("OTEL_EXPORTER_OTLP_HEADERS_AUTHORIZATION"))
            .withEnv("OTEL_TRACES_EXPORTER_ENABLED", "true")
            .withEnv("OTEL_LOGS_EXPORTER_ENABLED", "true")
            .withEnv("OTEL_METRICS_EXPORTER_ENABLED", "true")
            .withDebugLogLevel();
        */
    }

    @Bean
    public DynamicPropertyRegistrar properties(@Nullable MicrocksContainer microcks) {
        return (registrar) -> {
            if (microcks != null) {
                registrar.add("spring.ai.anthropic.base-url", () -> microcks.getRestMockEndpoint("Anthropic API", "0.83.0"));
            }
        };
    }
}
