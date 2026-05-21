# Spring AI + MCP + OpenTelemetry

In this step you add event-driven capabilities to the store using [Apache Kafka](https://kafka.apache.org/). When an order is placed, we should now call the 3rd-party `shipping` component responsible of shipments. When shipment is recorded, an event is published to a Kafka topic, and clients receive real-time updates over a WebSocket connection. The store publishes and consumes via Spring Kafka's `KafkaTemplate` and `@KafkaListener`; the shipping service publishes via the `segmentio/kafka-go` Kafka client.

Because we're building an AI-infused app, we want to integrate with `shipping` using MCP! However, `shipping` is 3rd-party and it proposed a gRPC service interface. To automatically, generate a MCP Server from this, we'll use [Reshapr](https://reshapr.io).

## What you will learn

- Wrapping the third-party `shipping` component as a MCP service
- Registering a new MCP tool into the app for shipment requests
- Subscribing to domain events and forwarding them to WebSocket clients
- Running Kafka locally via Testcontainers for integration tests
- Testing event-driven architectures without coupling to a specific broker

## Architecture

```
Browser ──► Store (port 8080)
              ├── Spring AI ChatClient → Anthropic Claude
                  └── Anthropic Claude (LLM)
                    └── MCP tools (remote, via HTTP)
                          └── Warehouse MCP Server (port 8087)
                                └── Warehouse REST API (port 8086)
                          └── Shipping MCP Server (port 7777)
                                └── Shipping gRPC API (port 9091)
                                    └── kafka-go producer ──► Kafka topic "shipments"
              ├── Kafka topic "shipments" ──► @KafkaListener
                  └── Events Rest Controller (/api/events)
                        └── WebSocket endpoint (/ws/events)
                              └── EventWebSocketHandler (real-time push to browser)
```

The store uses Spring Kafka (`KafkaTemplate` + `@KafkaListener`) to publish and consume shipment events directly from Kafka, with no middleware sidecar in between.

## Prerequisites

- Java 21 — [adoptium.net](https://adoptium.net/temurin/releases/?version=21)
- Maven — [maven.apache.org](https://maven.apache.org/download.cgi)
- Node.js v22+ and npm — [nodejs.org](https://nodejs.org/en/download)
- Docker — [docker.com](https://www.docker.com/products/docker-desktop/)
- An Anthropic API key — [console.anthropic.com](https://console.anthropic.com/)


## Running the tests

The test suite starts a Kafka container via Testcontainers and lets Spring Boot autoconfigure the producer/consumer via `@ServiceConnection`:

```bash
cd step-03/store
export ANTHROPIC_API_KEY=your-key-here
mvn test
```

**What to look for in the code:**
- `ContainersConfig.java` — we start a `KafkaContainer` annotated with `@ServiceConnection` so Spring Boot wires the bootstrap servers automatically.
- `StoreTests.java` — we publish via `KafkaTemplate` and verify the `@KafkaListener` receives the message.

## Key source files

| File | Description |
|---|---|
| `EventsRestController` | Hosts a `@KafkaListener` for the `shipments` topic and a `/mock` endpoint to publish events via `KafkaTemplate` |
| `EventWebSocketHandler` | Handles WebSocket connections and pushes order events to clients |
| `WebSocketConfig` | Registers the WebSocket handler at `/ws/events` |
| `Event` | Domain event class published to the Kafka `shipments` topic |


## Exercices

### Exercice T1: Run store app against a local Kafka broker

**Goal:** Run the store service against a local Kafka broker started by Testcontainers, and verify the store correctly consumes shipment events published to the `shipments` topic.

**What to look for in the code:**
- `ContainersConfig.java` — the `KafkaContainer` annotated with `@ServiceConnection`
- `EventsRestController.java` — the `@KafkaListener` that consumes `shipments` and the `/mock` endpoint that uses `KafkaTemplate.send`

Start the store with the Testcontainers-managed Kafka broker:

```bash
cd step-03/store

mvn spring-boot:test-run
```

Open your browser at [http://localhost:8080](http://localhost:8080). Publish a mock event (for example via `POST /api/events/mock`) and observe real-time WebSocket events in the UI.

Congrats! You just validated that your store app can receive Kafka messages and transmit them to the UI!

### Exercice R1: Run the Infrastructure Components

**Goal:** Prepare the infrastructure for runn all the components locally. As we rely on a bunch of components, we made your life easy so that you'll just have to run the store appl in the next steps.

> [!NOTE]
> You may still have a `jaeger` container running as we've asked Testcontainers to reuse previous instances. You can now safely stop it to save a few resources.

```bash
cd step-03

docker compose up -d
```

This should produce something similar to the following output:

```bash
[+] up 7/7
 ✔ Container shipping              Started
 ✔ Container kafka                 Started
 ✔ Container reshapr-postgres      Started
 ✔ Container jaeger                Started
 ✔ Container reshapr-control-plane Healthy
 ✔ Container reshapr-gateway-01    Started
```

You can inspect the different containers that are running here. We run the shipping 3rd-party service as a container and we have started the reshapr containers for generating an MCP Server for it.

### Exercice A1: Add a new shipping-mcp MCP Server

**Goal:** Learn how Reshapr can easily generate an MCP Server from an API specification.

**Steps:**

1. **Be sure to have the infrastructure up and running** — this was the previous exercise outcome.

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
   reshapr import -f ./shipping-mcp/shipping-service.proto --be http://shipping:9091
   ```

   You should get the folowwing output:
   ```bash
   ✅ Import successful!
   ℹ️  Discovered Service springio.workshop.v1.ShippingService with ID: 0Q18650R54AFJ
   ✅ Exposition done!
   ✅ Exposition is now active!
   Exposition ID  : 0Q1JVWGQPZKQ9
   Organization   : reshapr
   Created on     : 2026-04-10T14:40:37.565477
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


### Exercice R2: Run the app and E2E test

**Goal:** Start the store application and see it connect the dots!

Start the store:

```bash
cd step-03/store

export ANTHROPIC_API_KEY=your-key-here
mvn spring-boot:run
```

Open your browser at [http://localhost:8080](http://localhost:8080).

Place an order and observe real-time WebSocket events in the UI. 🎉

Check `jaeger` to explore the different traces and spans created by the different components.

