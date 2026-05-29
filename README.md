# Spring AI + MCP + OpenTelemetry Webinar

## Introduction

Building agentic applications using Spring AI with MCP servers to connect agents with existing services, then dive into two areas that are easy to overlook but hard to fix later: testing your agents with [Microcks](https://microcks.io), observing them with OpenTelemetry, and understanding MCP interactions at scale with [Reshapr](https://reshapr.io).

## Getting Started

Start by cloning the repository and entering the project directory:

```bash
git clone https://github.com/salaboy/spring-ai-agentic-webinar.git
cd spring-ai-agentic-webinar
```

---

## Prerequisites

All steps require the following tools. Install them before starting.

### Java 21

Required for all Spring Boot services.

- **macOS:** `brew install openjdk@21`
- **Linux/Windows:** Download from [Adoptium](https://adoptium.net/temurin/releases/?version=21) or [Oracle](https://www.oracle.com/java/technologies/downloads/#java21)

Verify: `java -version`

### Maven

[maven.apache.org](https://maven.apache.org/download.cgi)

### Node.js v22+ and npm

[nodejs.org](https://nodejs.org/en/download)

### Docker (with Docker Compose)

Used to run infrastructure services (Jaeger, Kafka, PostgreSQL, Dapr sidecars) during tests and local development.

- **All platforms:** Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)

Verify: `docker --version && docker compose version`

### Anthropic API Key

The store application uses [Anthropic Claude](https://www.anthropic.com/claude) as its LLM. You need an API key.

1. Sign up or log in at [console.anthropic.com](https://console.anthropic.com/)
2. Create an API key in your account settings
3. Export it in your shell: `export ANTHROPIC_API_KEY=your-key-here`

### Dash0 trial environment

The store application and the infrastructure uses [Dash0](https://www.dash0.com) as its OpenTelemetry. You need a DataSet and the associated API key.

1. Sign up or log in at [app.dash0.com](https://app.dash0.com/)
2. Create a `DataSet` and an `Auth Token` in your account
3. Retrieve the OpenTelemetry Exporter address
4. Export those environment variables in your shell:
```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=<your-exporter-url-here-with-https>
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer <your-auth-token-here>,Dash0-Dataset=<your-dataset-here>" 
export OTEL_EXPORTER_OTLP_HEADERS_AUTHORIZATION="Bearer <your-auth-token-here>"
export DASH0_OTLP_ENDPOINT=<your-exporter-host-here-with-port-4317>
export DASH0_AUTH_TOKEN=<your-auth-token-here>
export DASH0_DATASET=<your-dataset-here>
```

---


## Architecture

```
Browser ──► Store (port 8080)
              ├── Spring AI ChatClient → Anthropic Claude
                  └── Anthropic Claude (LLM)
                    └── MCP tools (remote, via HTTP)
                          └── Shipping MCP Server (port 7777)
                                └── Shipping gRPC API (port 9091)
                                    └── kafka-go producer ──► Kafka topic "shipments"
              ├── Kafka topic "shipments" ──► @KafkaListener
                  └── Events Rest Controller (/api/events)
                        └── WebSocket endpoint (/ws/events)
                              └── EventWebSocketHandler (real-time push to browser)
```

The store uses Spring Kafka (`KafkaTemplate` + `@KafkaListener`) to consume shipment events directly from Kafka, with no middleware sidecar in between.

## Key source files

| File | Description |
|---|---|
| `EventsRestController` | Hosts a `@KafkaListener` for the `shipments` topic and a `/mock` endpoint to publish events via `KafkaTemplate` |
| `EventWebSocketHandler` | Handles WebSocket connections and pushes order events to clients |
| `WebSocketConfig` | Registers the WebSocket handler at `/ws/events` |
| `Event` | Domain event class published to the Kafka `shipments` topic |

## Understanding & Testing our Agentic Apps

```sh
cd store
mvn clean -Dspring-boot.run.jvmArguments="-Dmicrocks.enabled=true" spring-boot:test-run
```

**What to look for in the code:**
- `ContainersConfig.java`:
  * we start a `KafkaContainer` annotated with `@ServiceConnection` so Spring Boot wires the bootstrap servers automatically.
  * we start a `MicrocksContainer` because of the `-Dmicrocks.enabled=true` property. We loade it with Anthropic API and mocks and override the default Anthropic SDK endpoint with a `DynamicPropertyRegistrar`
- `StoreTests.java` — we publish via `KafkaTemplate` and verify the `@KafkaListener` receives the message.

Open your browser at [http://localhost:8080](http://localhost:8080). 

* Publish a mock event (for example via `POST /api/events/mock`) and observe real-time WebSocket events in the UI.
* Ask in the chat to `List all items` and see Microcks answering instead of Anthropic Claude.

> [!NOTE]
> Inspect all traces, logs and metrics on Dash0 console.


## Wiring tools to existing APIs with MCP

Here we're using Docker Compose to run all the infrastructure components locally. 

```bash
docker compose up -d
```

This should produce something similar to the following output:

```bash
[+] up 8/8
 ✔ Network spring-ai-agentic-webinar_default Created                            0.0s
 ✔ Container kafka                           Started                            0.3s
 ✔ Container reshapr-postgres                Started                            0.3s
 ✔ Container jaeger                          Started                            0.3s
 ✔ Container reshapr-control-plane           Healthy                            7.9s
 ✔ Container otel-collector                  Started                            0.4s
 ✔ Container shipping                        Started                            0.4s
 ✔ Container reshapr-gateway-01              Started                            7.9s
```

You can inspect the different containers that are running here. We run the shipping 3rd-party service as a container and we have started the reshapr containers for generating an MCP Server for it.

### Add a new shipping-mcp MCP Server

**Goal:** Learn how Reshapr can easily generate an MCP Server from an API specification.

**Steps:**

1. **Be sure to have the infrastructure up and running** — this was the previous outcome.

2. Install the Reshapr CLI:

    ```bash
    npm install -g @reshapr/reshapr-cli
    ```

3. Login to the reshapr control-plane (running on port 5555):

   ```bash
   reshapr login -s http://localhost:5555 -u admin -p password
   ````

   You should get the following output:
   ```bash
   ℹ️  Logging in to Reshapr at http://localhost:5555...
   ✅ Login successful!
   ℹ️  Welcome, admin!
   ✅ Configuration saved to /Users/<you>/.reshapr/config   
   ```   

4. Import the `shipping-service.proto` file and expose it as an MCP Server wrapping shipping running on port 9091:

   ```bash
   reshapr import -f ./shipping-mcp/shipping-service.proto --be http://shipping:9091 --audit
   ```

   You should get the folowwing output:
   ```bash
   ✅ Import successful!
   ℹ️  Discovered Service springio.workshop.v1.ShippingService with ID: 0Q18650R54AFJ
   ✅ Exposition done!
   ✅ Exposition is now active!
   Exposition ID  : 0Q1JVWGQPZKQ9
   Organization   : reshapr
   Created on     : 2026-05-28T13:56:30.421801
   Service ID     : 0Q18650R54AFJ
   Service Name   : springio.workshop.v1.ShippingService
   Service Version: v1
   Service Type   : GRPC -> http://shipping:9091
   Endpoints      : localhost:7777/mcp/reshapr/springio.workshop.v1.ShippingService/v1
   ```

5. Using a MCP Client you can then test the configured endpoint. You can, for example, run [MCPJam](https://www.mcpjam.com/) for that:

   ```bash
   npx @mcpjam/inspector@latest
   ```

6. **Bonus:** Reshapr can also wrap REST or GraphQL API as MCP Servers. Try to import the `https://raw.githubusercontent.com/open-meteo/open-meteo/refs/heads/main/openapi.yml` file and use `https://api.open-meteo.com` as the backend endpoint.


### Run the app and E2E test

**Goal:** Start the store application and see it connect the dots!

Start the store, this time in `run` mode:

```bash
cd store
mvn spring-boot:run
```

Open your browser at [http://localhost:8080](http://localhost:8080).

Place an order and observe real-time WebSocket events in the UI. 🎉

> [!NOTE]
> Inspect all traces, logs and metrics on Dash0 console.
