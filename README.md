# SchemaFlow

A Spring Boot event-driven application that demonstrates how **Apache Kafka**, **Apache Avro**, and **Confluent Schema Registry** can be used together to build reliable producer-consumer communication with strongly typed message contracts.

The project shows a complete flow from a REST API request to Kafka, schema validation, event consumption, and PostgreSQL persistence.

---

## Why this project?

In event-driven systems, producers and consumers are often deployed independently.

Without a shared contract, even a small change to a message can unexpectedly break downstream services.

**SchemaFlow** solves this by using **Avro schemas as contracts** and **Confluent Schema Registry** to manage and validate those schemas.

This project demonstrates:

* Schema-based Kafka messaging using **Apache Avro**
* Schema registration with **Confluent Schema Registry**
* Safe schema evolution
* Spring Boot producer and consumer services
* REST-to-Kafka event publishing
* Kafka-to-PostgreSQL event persistence
* DTO-to-Avro and Avro-to-Entity mapping with **MapStruct**
* Local infrastructure using **Docker Compose**
* Support for local Kafka and Confluent Cloud configurations

---

## Architecture

```text
                    POST /users
                         │
                         ▼
              ┌─────────────────────┐
              │    Producer App     │
              │      Port 8080      │
              │                     │
              │ REST → DTO → Avro   │
              └──────────┬──────────┘
                         │
                         │ Avro Event
                         ▼
              ┌─────────────────────┐
              │    Apache Kafka     │
              │   Topic: users.v1   │
              └──────────┬──────────┘
                         │
             Schema ID   │
          ┌──────────────┘
          ▼
┌─────────────────────────┐
│ Confluent Schema        │
│ Registry                │
│ Port 8081               │
└─────────────────────────┘

                         │
                         ▼
              ┌─────────────────────┐
              │    Consumer App     │
              │      Port 8089      │
              │                     │
              │ Avro → Entity → JPA │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │     PostgreSQL      │
              │      Port 5432      │
              │                     │
              │    users.contact    │
              └─────────────────────┘
```

---

## How the flow works

### 1. Receive the request

The producer exposes:

```http
POST /users
```

A JSON request is received by the Spring Boot REST controller.

Example:

```json
{
  "id": "u-1001",
  "email": "ada@example.com",
  "phone": "5551234567",
  "firstName": "Ada",
  "lastName": "Lovelace",
  "isActive": true,
  "age": 36
}
```

### 2. Convert the request to Avro

The incoming DTO is converted into a generated Avro `User` object using **MapStruct**.

The Avro class is generated from:

```text
common-schemas/src/main/avro/User.avsc
```

This schema acts as the contract between the producer and consumer.

### 3. Publish to Kafka

The producer sends the Avro message to:

```text
users.v1
```

`KafkaAvroSerializer` communicates with Schema Registry and associates the message with its registered schema.

The serialized Kafka payload contains the Schema Registry schema ID, allowing consumers to determine which schema was used to write the event.

### 4. Consume the event

The consumer listens to the same Kafka topic:

```text
users.v1
```

`KafkaAvroDeserializer` retrieves the corresponding schema information and converts the event back into a strongly typed Avro object.

### 5. Persist to PostgreSQL

The consumer maps the Avro object into a JPA entity and stores it in:

```text
users.contact
```

---

## Tech Stack

| Category              | Technology                  |
| --------------------- | --------------------------- |
| Language              | Java 21                     |
| Framework             | Spring Boot 3.5.6           |
| Messaging             | Apache Kafka                |
| Kafka Integration     | Spring Kafka                |
| Serialization         | Apache Avro 1.11.4          |
| Schema Management     | Confluent Schema Registry   |
| Persistence           | PostgreSQL                  |
| ORM                   | Spring Data JPA / Hibernate |
| Object Mapping        | MapStruct 1.6.3             |
| Boilerplate Reduction | Lombok                      |
| Build Tool            | Maven                       |
| Testing               | JUnit 5 / Mockito           |
| Infrastructure        | Docker Compose              |

---

## Project Structure

```text
SchemaFlow/
│
├── common-schemas/
│   ├── pom.xml
│   └── src/main/avro/
│       └── User.avsc
│
├── producer-app/
│   ├── src/main/java/
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── application-cloud.yml
│   └── pom.xml
│
├── consumer-app/
│   ├── src/main/java/
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   └── application-cloud.yml
│   └── pom.xml
│
├── docker/
│   ├── docker-compose.yml
│   └── postgres/
│       └── init.sql
│
├── postman/
│   └── kafka-schema-registry-spring-demo.postman_collection.json
│
├── .gitignore
├── pom.xml
└── README.md
```

### Module responsibilities

**`common-schemas`**

Contains the Avro schema and generates the Java model used by the producer and consumer.

**`producer-app`**

Accepts REST requests, maps the input to Avro, and publishes events to Kafka.

**`consumer-app`**

Consumes Kafka events, deserializes Avro records, maps them to JPA entities, and stores them in PostgreSQL.

**`docker`**

Contains the local Kafka, Zookeeper, Schema Registry, and PostgreSQL infrastructure.

---

## Prerequisites

Before running the project locally, install:

* **Java 21**
* **Maven 3.9+**
* **Docker Desktop**
* **Git**

Verify:

```bash
java -version
mvn -version
docker --version
git --version
```

---

# Getting Started

## 1. Clone the repository

```bash
git clone <your-repository-url>
cd SchemaFlow
```

---

## 2. Start the infrastructure

From the project root:

```bash
docker compose -f docker/docker-compose.yml up -d
```

This starts:

| Service         | Address                 |
| --------------- | ----------------------- |
| Kafka           | `localhost:29092`       |
| Schema Registry | `http://localhost:8081` |
| PostgreSQL      | `localhost:5432`        |
| Zookeeper       | `localhost:2181`        |

Check running containers:

```bash
docker ps
```

You should see containers similar to:

```text
kafka
zookeeper
schema-registry
postgres-users
```

---

## 3. Verify Schema Registry

Open:

```text
http://localhost:8081/subjects
```

Or:

```bash
curl http://localhost:8081/subjects
```

Before the first event is published, the response may be:

```json
[]
```

---

## 4. Build the project

Run from the project root:

```bash
mvn clean install
```

This is important because the `common-schemas` module generates the Java Avro classes required by the producer and consumer.


## 5. Start the consumer

Open a new terminal:

```bash
cd consumer-app
mvn spring-boot:run
```

The consumer runs on:

```text
http://localhost:8089
```

---

## 6. Start the producer

Open another terminal:

```bash
cd producer-app
mvn spring-boot:run
```

The producer runs on:

```text
http://localhost:8080
```

---

## 7. Publish a user event

Send:

```http
POST http://localhost:8080/users
```

Example with cURL:

```bash
curl -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{
    "id": "u-1001",
    "email": "ada@example.com",
    "phone": "5551234567",
    "firstName": "Ada",
    "lastName": "Lovelace",
    "isActive": true,
    "age": 36
  }'
```

You can also import the included Postman collection from:

```text
postman/
```

---

## 8. Verify the schema

After publishing the first event:

```bash
curl http://localhost:8081/subjects
```

You should see a subject similar to:

```json
[
  "users.v1-value"
]
```

---

## 9. Verify PostgreSQL

Connect to PostgreSQL:

```bash
docker compose -f docker/docker-compose.yml exec postgres \
  psql -U kafka -d users
```

Then run:

```sql
SELECT * FROM users.contact;
```

The consumed event should appear in the table.

---

# Avro Schema

The main event contract is:

```text
common-schemas/src/main/avro/User.avsc
```

Example:

```json
{
  "type": "record",
  "namespace": "com.mathias.kafka.schema",
  "name": "User",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "phone",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "firstName",
      "type": "string",
      "default": ""
    },
    {
      "name": "lastName",
      "type": "string",
      "default": ""
    },
    {
      "name": "isActive",
      "type": "boolean",
      "default": true
    },
    {
      "name": "createdAt",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "age",
      "type": "int",
      "default": 0
    }
  ]
}
```

---

# Schema Evolution

One of the main goals of this project is to demonstrate how Kafka event contracts can evolve safely.

For example, suppose we want to add:

```json
{
  "name": "nickname",
  "type": ["null", "string"],
  "default": null
}
```

Because the field is optional and has a default value, older consumers can continue processing messages without requiring an immediate deployment.

After changing the schema, rebuild:

```bash
mvn clean install
```

Schema Registry can then validate whether the new schema is compatible with previous versions.

This allows producers and consumers to evolve more independently instead of requiring every service to be deployed at the same time.

---

# Local Configuration

## Producer

The producer is configured in:

```text
producer-app/src/main/resources/application.yml
```

Important properties:

```yaml
server:
  port: 8080

app:
  topic: users.v1

spring:
  kafka:
    bootstrap-servers: localhost:29092
    properties:
      schema.registry.url: http://localhost:8081
      auto.register.schemas: true
```

---

## Consumer

The consumer is configured in:

```text
consumer-app/src/main/resources/application.yml
```

Important properties:

```yaml
server:
  port: 8089

app:
  topic: users.v1

spring:
  kafka:
    bootstrap-servers: localhost:29092
    consumer:
      group-id: user-svc
    properties:
      schema.registry.url: http://localhost:8081
      specific.avro.reader: true
```

---

# Confluent Cloud

Both applications also include:

```text
application-cloud.yml
```

These profiles can be used to configure the services for **Confluent Cloud** instead of the local Docker-based Kafka environment.

Keep credentials outside source control using environment variables or local configuration files.

> **Never commit Kafka API keys, Schema Registry credentials, database passwords, or `.env` files containing real secrets.**

Recommended `.gitignore` entries:

```gitignore
.env
**/.env
*.local.yml
```

---

# Useful Commands

### Start infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
```

### Stop infrastructure

```bash
docker compose -f docker/docker-compose.yml down
```

### Check containers

```bash
docker ps
```

### Build everything

```bash
mvn clean install
```

### Run producer

```bash
cd producer-app
mvn spring-boot:run
```

### Run consumer

```bash
cd consumer-app
mvn spring-boot:run
```

### List Schema Registry subjects

```bash
curl http://localhost:8081/subjects
```

---

# Troubleshooting

### `cannot find symbol: class User`

The Avro-generated classes have not been built yet.

Run from the project root:

```bash
mvn clean install
```

---

### Consumer receives no events

Check that all containers are running:

```bash
docker ps
```

Then confirm that:

* Kafka is running on `localhost:29092`
* Schema Registry is running on `localhost:8081`
* Producer and consumer use the same topic
* The consumer group is active

---

### Schema Registry is unavailable

Check its logs:

```bash
docker compose -f docker/docker-compose.yml logs schema-registry
```

If Kafka was still starting when Schema Registry launched:

```bash
docker compose -f docker/docker-compose.yml restart schema-registry
```

---

### Port already in use

The project uses:

```text
8080  Producer
8089  Consumer
8081  Schema Registry
29092 Kafka
5432  PostgreSQL
2181  Zookeeper
```

Make sure another application is not already using one of these ports.

---

# Key Design Choices

### Avro instead of plain JSON

Avro provides a strongly defined contract and compact binary serialization.

Instead of discovering incompatible message changes at runtime, schema compatibility can be checked before those changes reach consumers.

### Schema Registry

Schema Registry provides centralized schema versioning and compatibility management.

The Kafka message only needs to reference the appropriate registered schema rather than carrying the full schema with every event.

### Separate producer and consumer applications

The producer and consumer are independent Spring Boot applications.

This mirrors real distributed systems where services are deployed separately and must communicate through stable contracts.

### MapStruct

MapStruct generates object-mapping code at compile time.

This keeps mapping logic simple while allowing schema or model differences to surface during development.

### `ddl-auto: validate`

Hibernate validates the existing database structure instead of automatically modifying it.

This makes database schema mismatches visible during application startup.

---

# What This Project Demonstrates

By building this project, I focused on practical concepts used in production event-driven systems:

* Event-driven microservices
* Apache Kafka producer/consumer architecture
* Schema-based messaging
* Avro serialization and deserialization
* Schema Registry integration
* Backward-compatible schema evolution
* REST API development with Spring Boot
* Spring Data JPA persistence
* PostgreSQL integration
* Docker-based local infrastructure
* Multi-module Maven projects
* Environment-specific application configuration

---

# Roadmap

Future improvements could include:

* Dead-letter topic handling
* Retry and error-recovery strategies
* Idempotent event consumption
* Integration testing with Testcontainers
* GitHub Actions CI/CD
* Automated schema compatibility checks
* Consumer lag monitoring
* Micrometer and Grafana metrics
* Distributed tracing
* Protobuf schema support

---

## Summary

**SchemaFlow** provides a simple end-to-end example of how a Spring Boot application can safely exchange strongly typed events using Kafka, Avro, and Schema Registry.

```text
REST API
   ↓
Spring Boot Producer
   ↓
Avro + Schema Registry
   ↓
Apache Kafka
   ↓
Spring Boot Consumer
   ↓
PostgreSQL
```


