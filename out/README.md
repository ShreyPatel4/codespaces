# Fiber SQS Cross-Region Latency Dataset

## Scenario
Synthetic telemetry for Fiber SQS investigating intermittent latency hitting Central-region provisioning traffic, requiring correlation across application logs, distributed traces, network metrics, infra stats, and TSO call queues.

## Tables
- Tier 1: clickhouse-fibersqs_app_logs, clickhouse-trace_spans, clickhouse-network_circuit_metrics, clickhouse-infra_host_metrics, clickhouse-tso_calls
- Tier 2 (optional): clickhouse-fibersqs_service_metrics_1m, clickhouse-network_events_alarms

## Incident Summary
Central provisioning flows intermittently degrade on the eastbound dependency during a multi-day window. Engineers must disentangle this from a Central CPU spike and a minor West deployment blip.

## Loading into ClickHouse
Example using `clickhouse-client`:

````bash
for file in data/clickhouse-*.csv; do
  table=$(basename "$file" | sed 's/clickhouse-//; s/.csv//')
  clickhouse-client --query "DROP TABLE IF EXISTS $table"
  clickhouse-client --query "CREATE TABLE $table (
    -- define schema matching README requirements
  ) ENGINE = MergeTree ORDER BY tuple()"
  clickhouse-client --query "INSERT INTO $table FORMAT CSVWithNames" < "$file"
done
````

## Realism Notes
- Diurnal traffic drivers shape txn volumes and retries
- Heavy-tailed latency + retry amplification during bursts
- Cross-region traces include clock skew, multi-span chains
- Network metrics emit bursty packet loss and RTT spikes
- Alerts mix true/false positives to mimic noisy operations
