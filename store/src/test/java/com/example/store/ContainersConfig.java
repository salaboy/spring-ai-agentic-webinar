package com.example.store;

import io.github.microcks.testcontainers.MicrocksContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.Nullable;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class ContainersConfig {

    @Bean
    Network network() {
        return Network.newNetwork();
    }

    @Bean(name = "jaegerContainer")
    GenericContainer<?> jaegerContainer(Network network) {
        return new GenericContainer<>(DockerImageName.parse("jaegertracing/jaeger"))
                .withNetwork(network)
                .withExposedPorts(16686, 4317, 4318)
                .withNetworkAliases("jaeger");
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
        return new MicrocksContainer("quay.io/microcks/microcks-uber:1.13.2-native")
            .withNetwork(network)
            .withMainArtifacts("anthropic-openapi.yaml")
            .withSecondaryArtifacts("anthropic-metadata.yaml", "anthropic-examples.yaml")
            .withEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")
            .withEnv("OTEL_TRACES_EXPORTER", "otlp")
            .withDebugLogLevel();
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
