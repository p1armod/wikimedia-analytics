# Wikimedia Real-Time Analytics Backend

A production-grade analytics backend that ingests live Wikipedia edit events and processes them through Apache Kafka.

## Architecture

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│  Wikimedia SSE   │     │  Producer        │     │  Kafka               │
│  EventStream     │────►│  Service (:8081) │────►│  wikimedia.          │
│  (recentchange)  │ SSE │  [Spring WebFlux]│     │   recentchange       │
└──────────────────┘     └─────────────────┘     └──────┬───────────────┘
                                                        │
                                              ┌─────────▼─────────┐
                                              │  Streams Processor │
                                              │  (:8082)           │
                                              │  [Kafka Streams]   │
                                              └─────────┬─────────┘
                                                        │
                              ┌──────────────────────────┼──────────────┐
                              ▼                          ▼              ▼
                    wikimedia.stats          wikimedia.top-wikis   wikimedia.alerts
                    wikimedia.bot-ratio      wikimedia.feed
                              │                          │              │
                              └──────────┬───────────────┘──────────────┘
                                         ▼
                              ┌──────────────────────┐
                              │  WebSocket Gateway   │
                              │  (:8080)             │
                              │  [Spring WebSocket]  │
                              │  + Redis + PostgreSQL│
                              └──────────────────────┘
```

## Prerequisites

- **Java 21** (JDK)
- **Docker** and **Docker Compose**
- **Maven 3.9+**

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

Wait for all containers to be healthy (~30 seconds):

```bash
docker-compose ps
```

### 2. Start Backend Services (in order)

```bash
# Terminal 1 — Producer
cd wikimedia-producer
mvn spring-boot:run

# Terminal 2 — Streams Processor
cd wikimedia-streams-processor
mvn spring-boot:run

# Terminal 3 — WebSocket Gateway
cd wikimedia-websocket-gateway
mvn spring-boot:run
```

## Kafka Topics

| Topic                    | Partitions | Purpose                                        |
|--------------------------|------------|-------------------------------------------------|
| `wikimedia.recentchange` | 6          | Raw edit events from Wikimedia SSE stream       |
| `wikimedia.stats`        | 3          | Aggregated edit stats (1-min tumbling window)   |
| `wikimedia.top-wikis`    | 3          | Top 10 wikis by edit count (1-min window)       |
| `wikimedia.bot-ratio`    | 3          | Bot vs human edit ratio (5-min window)          |
| `wikimedia.alerts`       | 1          | Spike detection alerts                          |
| `wikimedia.feed`         | 3          | Rate-limited raw events for live feed display   |

## WebSocket Topics (STOMP)

| STOMP Destination   | Payload          | Update Frequency |
|---------------------|------------------|------------------|
| `/topic/stats`      | `EditStats`      | Every 1 minute   |
| `/topic/top-wikis`  | `List<WikiStat>` | Every 1 minute   |
| `/topic/bot-ratio`  | `BotRatioStat`   | Every 5 minutes  |
| `/topic/alerts`     | `AlertEvent`     | On spike detect  |
| `/topic/feed`       | `WikimediaEvent` | Max 5/sec        |

**WebSocket Endpoint:** `ws://localhost:8080/ws` (with SockJS fallback)

## REST API Reference

| Endpoint                           | Method | Description                            |
|------------------------------------|--------|----------------------------------------|
| `/api/history/stats?from=&to=`     | GET    | Last 24h edit stats from PostgreSQL    |
| `/api/history/top-wikis`           | GET    | Latest top-wikis snapshot from Redis   |
| `/api/cache/stats`                 | GET    | Latest EditStats from Redis            |
| `/api/cache/bot-ratio`             | GET    | Latest BotRatioStat from Redis         |

## Environment Variables

| Variable                    | Default                          | Service     |
|-----------------------------|----------------------------------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS`   | `localhost:9092`                 | All backend |
| `POSTGRES_HOST`             | `localhost`                      | Gateway     |
| `POSTGRES_PORT`             | `5432`                           | Gateway     |
| `POSTGRES_DB`               | `wikimedia_analytics`            | Gateway     |
| `POSTGRES_USER`             | `wikimedia`                      | Gateway     |
| `POSTGRES_PASSWORD`         | `wikimedia`                      | Gateway     |
| `REDIS_HOST`                | `localhost`                      | Gateway     |
| `REDIS_PORT`                | `6379`                           | Gateway     |
| `WIKIMEDIA_STREAM_URL`      | `https://stream.wikimedia.org/…` | Producer    |
| `CORS_ALLOWED_ORIGINS`      | `http://localhost:5173`          | Gateway     |

## Monitoring

- **Kafka UI:** [http://localhost:9090](http://localhost:9090)
- **Producer Actuator:** [http://localhost:8081/actuator/health](http://localhost:8081/actuator/health)
- **Streams Actuator:** [http://localhost:8082/actuator/health](http://localhost:8082/actuator/health)
- **Gateway Actuator:** [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health)
