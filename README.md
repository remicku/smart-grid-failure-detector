# IDE1 Groupe 18

## Project — Smart Grid Failure Prediction

### Use case overview

Millions of sensors deployed across the electrical grid (transformers) continuously emit telemetry data. The platform serves two purposes: predicting outages before they occur and triggering automated rebalancing actions, while building a long-term analytics layer to forecast consumption.

### Preliminary questions

#### 1.a	What technical/business constraints should the data storage component of the program architecture meet to fulfill the requirement described by the customer in paragraph «Statistics» ?

The long-term analytics service imposes the following constraints on the storage layer:

-   **Volume**: with millions of sensors emitting every few seconds, the system must handle ~200 GB/day and accumulate several terabytes over months and years. Storage must be **horizontally scalable**.
-   **Variety**: data originates from heterogeneous sources. The storage layer must accommodate **varied schemas and data formats**.
-   **Batch-oriented access**: statistical analyses involve aggregations over large time windows. The storage must be **optimized for batch reads**, ideally using a columnar format.
-   **Long-term retention**: historical data must be preserved for regulatory compliance and multi-year trend analysis, requiring **durable, cost-efficient storage**.
-   **Query-readiness**: data must be cleaned, normalized, and structured to be directly consumable by analytical tools without additional preprocessing at query time.

#### 1.b	So what kind of component(s) (listed in the lecture) will the architecture need?

-   A **Data Lake** following a medallion architecture (Bronze / Silver / Gold layers), and covering raw ingestion, cleaned data, and aggregated business-ready datasets.
-   A **distributed file storage system** for the Bronze and Silver layers.
-   A **batch processing engine** for ETL pipelines, aggregations, and ML feature engineering.

#### 2.a	What business constraint should the architecture meet to fulfill the requirement describe in the paragraph «Alert»? 


The predictive alert service is mission-critical and imposes strict operational constraints:

-   **Low latency**: an outage prediction or grid overload must be detected and acted upon within seconds. Any delay can result in cascading failures affecting thousands of households or industrial customers.
-   **Continuous stream processing**: sensor data arrives as a high-throughput, unbounded stream. The architecture must process events **as they arrive**, without batching delays.
-   **High availability**: the alert pipeline cannot afford downtime. The system must be **fault-tolerant and redundant** by design.
-   **Velocity**: the system must sustain millions of concurrent data producers without message loss, even during peak load periods (e.g. heatwaves, winter demand spikes).

#### 2.b	Which component to choose?

-   A **distributed message broker** as the central ingestion layer. It guarantees high-throughput ingestion, message durability, replay capability, and decoupling between producers (sensors) and consumers.
-   A **stream processing engine** as the real-time consumer. It evaluates sliding time windows, applies anomaly detection models, and triggers alerts when thresholds are breached.
-   A second **sink consumer** that writes the raw stream to the Data Lake for long-term storage and batch reprocessing.

## Run

Start Kafka:

```bash
docker compose up -d
```

Start the simulator:

```bash
sbt "simulator/run"
```

In another terminal, read Kafka and write Bronze:

```bash
BRONZE_MAX_MESSAGES=20 sbt "bronzeIngestor/run"
```

Then build Silver and Gold:

```bash
sbt "datalake/runMain smartgrid.datalake.DataLakeJob"
```

Then run the statistics job:

```bash
sbt "analytics/runMain smartgrid.analytics.GoldAnalyticsJob"
```

The files are written under `data/bronze`, `data/silver`, `data/gold`, and `data/stats`.
The small page is `data/stats/index.html`.
