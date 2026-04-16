# Snowflake Streaming Architecture: Learning Guide

This document explains the "Streaming" architecture implemented in the ULEZ project, comparing it to other Snowflake patterns to help you choose the best one for future projects.

## 1. Batch vs. Continuous Ingestion

In your previous project, you likely used **Batch Ingestion**:
-   A script runs every hour/day.
-   It loads all available files at once.
-   High latency, but very predictable costs.

In this project, we simulated **Continuous Ingestion**:
-   Data moves as soon as it arrives.
-   Low latency, potentially higher costs if not managed carefully.

---

## 2. Snowflake Technology Stack

### **A. Snowpipe (Managed Ingestion)**
-   **How it works:** You point Snowflake to an S3/Azure/GCS bucket. As soon as a file lands, Snowflake automatically detects it and runs a `COPY INTO`.
-   **Cost:** Snowflake charges a small managed service fee plus compute time.
-   **Learning:** This is the most "hands-off" way to do streaming in Snowflake.

### **B. Streams & Tasks (Our Implementation)**
-   **Streams:** Act as a "CD" (Change Data) tracker. They don't copy data; they just point to what's new since the last time you read them.
-   **Tasks:** A scheduler. By using `WHEN SYSTEM$STREAM_HAS_DATA('my_stream')`, we ensure the task only runs when there is something to do.
-   **Why we used it:** It's the best way for a learner to see the "guts" of the pipeline. You manually see the stream capture changes and the task process them.

### **C. Dynamic Tables**
-   **How it works:** You define a SQL query (like your dbt model) and Snowflake handles the refresh automatically to meet a "Target Lag" (e.g., 1 minute).
-   **Cost:** Can be more expensive because the monitoring is handled by Snowflake 24/7.
-   **Learning:** This is the "Modern" declarative approach.

---

## 3. The "Lambda" vs "Kappa" Concept

-   **Lambda Architecture:** You have a "Fast" layer (streaming) and a "Batch" layer (historical). They merge at the end.
-   **Kappa Architecture:** You treat *everything* as a stream. Long-term storage is just a very long replayable stream.

**Our simulation follows a "Kappa-lite" approach:** By treating each brand file as a new event in the stream, we keep the data engineering logic clean and reactive.
