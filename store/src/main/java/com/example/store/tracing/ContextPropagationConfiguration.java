package com.example.store.tracing;

import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.customizer.McpSyncHttpClientRequestCustomizer;
import io.modelcontextprotocol.common.McpTransportContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;

import org.springframework.ai.mcp.customizer.McpClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.support.ContextPropagatingTaskDecorator;

import java.net.URI;
import java.net.http.HttpRequest;

@Configuration(proxyBeanMethods = false)
public class ContextPropagationConfiguration {

    @Bean
    ContextPropagatingTaskDecorator contextPropagatingTaskDecorator() {
        return new ContextPropagatingTaskDecorator();
    }

    @Bean
    McpClientCustomizer<HttpClientStreamableHttpTransport.Builder> tracePropagationMcpClientCustomizer(OpenTelemetry openTelemetry) {
        return (name, transportBuilder) ->
              transportBuilder.httpRequestCustomizer(new McpSyncHttpClientRequestCustomizer() {
                  @Override
                  public void customize(HttpRequest.Builder builder, String method, URI endpoint, String body, McpTransportContext context) {
                      openTelemetry.getPropagators().getTextMapPropagator().inject(
                          Context.current(),
                          builder,
                          (b, key, value) -> b.header(key, value)
                     );
                  }
              });
    }
}
