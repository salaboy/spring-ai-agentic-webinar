package com.example.store.tracing;

import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.customizer.McpSyncHttpClientRequestCustomizer;
import io.modelcontextprotocol.common.McpTransportContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;

import org.springframework.ai.mcp.customizer.McpClientCustomizer;
import org.springframework.boot.restclient.RestClientCustomizer;
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

    /**
     * Configures a RestClientCustomizer to propagate OpenTelemetry context.
     * Was working until Spring-AI 2.0.0-M3 but now broken, likely due to changes in Anthropic Java SDK 
     * that's is now using OkHttp instead of Spring's RestTemplate under the hood. This is a known issue and will be fixed in a future release.
     */
    @Bean
    RestClientCustomizer tracePropagationRestClientCustomizer(OpenTelemetry openTelemetry) {
        return builder -> builder.requestInterceptor((request, body, execution) -> {
            openTelemetry.getPropagators().getTextMapPropagator().inject(
                    Context.current(),
                    request.getHeaders(),
                    (headers, key, value) -> headers.set(key, value)
            );
            return execution.execute(request, body);
        });
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
