# Bronze Layer Ingestion - Layer 3 Spark Parallel Processing

**High-Performance Distributed Ingestion for Databricks Lakehouse**

---

## Overview

Layer 3 Spark Parallel is a production-grade ingestion framework leveraging Spark's distributed processing engine for high-volume file ingestion. Built for MercuryGate Gold data feeds, this implementation provides 10-20x performance improvements over serial processing while maintaining complete audit trails and enterprise-grade observability.

**Version:** 3.0.0  
**Status:** Production Ready  
**Last Updated:** February 12, 2026  
**Compute Support:** Serverless, Job Compute, All-Purpose, SQL Warehouse, DLT Pipeline

---

## Architecture

### Processing Model

Layer 3 uses Spark's native parallel processing capabilities to ingest multiple files simultaneously:

```
Multiple Files → Spark Read (Wildcard) → Distributed Processing → Single Write → Bronze Table
     ↓                                              ↓
Archive Batch                              Track Per-File Metrics
```

**Key Differences from Layer 2:**
- Reads all files at once using wildcard patterns
- Spark automatically distributes work across executors
- Single consolidated write operation
- Per-file audit tracking preserved for compatibility

### Performance Characteristics

| Files | Serial (Layer 2) | Parallel (Layer 3) | Speedup |
|-------|------------------|-------------------|---------|
| 10    | 20 seconds       | 2 seconds         | 10x     |
| 100   | 200 seconds      | 10 seconds        | 20x     |
| 500   | 1,000 seconds    | 30 seconds        | 33x     |
| 1,000 | 2,000 seconds    | 60 seconds        | 33x     |

---

## Features

### Core Capabilities

- **Spark Distributed Processing** - Parallel file ingestion across cluster executors
- **Dynamic Schema Loading** - Central schema registry integration
- **Source File Tracking** - Unity Catalog compatible metadata tracking
- **Dual Audit Strategy** - Batch-level and per-file audit tables
- **Cost Transparency** - Complete cost tracking across all compute types
- **Idempotency** - Safe to retry, prevents duplicate processing
- **Production Exit Codes** - Proper job orchestration integration

### Audit Tables

The framework populates six audit tables for complete observability:

**Batch-Level Tracking:**
1. `batch_processing_audit` - Overall batch metrics
2. `batch_file_details` - File list in each batch

**Per-File Tracking:**
3. `file_processing_audit` - Individual file audit records
4. `performance_metrics` - Per-file throughput metrics
5. `notifications` - Alerts, warnings, schema changes
6. `ingestion_costs` - Detailed cost breakdown by file

**Reference Data:**
7. `cost_rates` - Current pricing for all compute types

### Compute Type Support

Automatically detects and optimizes for:
- **Serverless** - Auto-scaling, lowest DBU cost ($0.07/DBU)
- **Job Compute** - Scheduled batch processing ($0.10/DBU)
- **All-Purpose** - Interactive development ($0.40/DBU)
- **SQL Warehouse** - SQL analytics workloads ($0.22/DBU)
- **DLT Pipeline** - Delta Live Tables integration ($0.20/DBU)

---

## Database Schema

### Bronze Tables

**Primary Data Table:**
```sql
dev_bronze.load_detail.load_transactions
```
Contains ingested Load Detail records with metadata columns:
- `_src_file` - Source filename
- `_src_file_path` - Full file path from Unity Catalog metadata
- `_ingestion_timestamp` - Processing timestamp
- `_execution_id` - Batch execution identifier
- `_schema_version` - Schema version from registry
- `processing_date` - Partition column

### Audit Tables

**Batch Processing Audit:**
```sql
dev_bronze.bronze_audit.batch_processing_audit
```
| Column | Type | Description |
|--------|------|-------------|
| execution_id | string | Unique batch identifier |
| domain | string | Data domain (Load Detail) |
| file_count | int | Files in batch |
| total_rows | bigint | Total rows processed |
| start_time | timestamp | Batch start time |
| end_time | timestamp | Batch completion time |
| duration_seconds | double | Total processing time |
| user_name | string | User who executed |
| status | string | SUCCESS or FAILED |
| processing_mode | string | spark_parallel |
| schema_version | int | Schema version used |

**File Processing Audit:**
```sql
dev_bronze.bronze_audit.file_processing_audit
```
| Column | Type | Description |
|--------|------|-------------|
| execution_id | string | Batch identifier |
| file_name | string | Individual file name |
| file_size | bigint | File size in bytes |
| file_modified_timestamp | timestamp | File modification time |
| start_time | timestamp | Processing start |
| end_time | timestamp | Processing completion |
| user_name | string | Executing user |
| status | string | SUCCESS or FAILED |
| rows_processed | bigint | Rows from this file |
| schema_info | string | Schema details (JSON) |
| file_properties | string | File properties (JSON) |
| processing_details | string | Processing metadata (JSON) |

**Performance Metrics:**
```sql
dev_bronze.bronze_audit.performance_metrics
```
| Column | Type | Description |
|--------|------|-------------|
| execution_id | string | Batch identifier |
| file_name | string | File name |
| file_size_mb | double | File size in MB |
| row_count | bigint | Rows processed |
| duration_seconds | double | Processing duration |
| rows_per_second | double | Throughput rate |
| mb_per_second | double | Data throughput |
| timestamp | timestamp | Metric timestamp |

**Cost Tracking:**
```sql
dev_bronze.cost_tracking.ingestion_costs
```
| Column | Type | Description |
|--------|------|-------------|
| execution_id | string | Batch identifier |
| file_name | string | File name |
| domain | string | Data domain |
| cluster_id | string | Cluster identifier |
| num_workers | int | Cluster size |
| compute_cost_usd | double | Compute costs |
| storage_cost_usd | double | Storage costs |
| total_cost_usd | double | Total cost |
| cost_per_row | double | Unit economics |
| driver_node_type | string | Driver VM type |
| worker_node_type | string | Worker VM type |
| created_timestamp | timestamp | Cost record time |

**Cost Rates Reference:**
```sql
dev_bronze.cost_tracking.cost_rates
```
| Column | Type | Description |
|--------|------|-------------|
| cost_type | string | DBU, VM, or Storage |
| category | string | Compute type or VM SKU |
| rate | double | Cost per unit |
| unit | string | Pricing unit |

---

## Configuration

### Schema Registry Integration

The notebook loads schema definitions from:
```sql
dev_bronze.metadata.schema_registry
```

Schema registry contains:
- Domain name
- Schema version
- Column definitions (name, type, nullable)
- File pattern matching
- Partitioning strategy
- Z-ordering columns

### Widget Parameters

| Widget | Values | Default | Description |
|--------|--------|---------|-------------|
| domain | Load Detail | Load Detail | Data domain to process |
| file_pattern | text | (empty) | Optional file filter |
| dedup_strategy | keep_all, keep_latest | keep_all | Deduplication approach |

### Deduplication Strategies

**keep_all (Recommended for Bronze):**
- Preserves all records including duplicates
- Maintains complete historical record
- Deduplication handled in Silver layer

**keep_latest:**
- Removes duplicates by `load_number`
- Keeps most recent `current_tender_timestamp`
- Use when source system sends duplicate updates

---

## Deployment

### Prerequisites

- Databricks Runtime 14.3 LTS or higher
- Unity Catalog enabled workspace
- Schema registry configured (`dev_bronze.metadata.schema_registry`)
- Storage mounted at `/mnt/mg-gold-raw-files`
- Catalog access: `dev_bronze` (read/write)

### Cluster Requirements

**Minimum Configuration:**
- 2 workers (4 cores each)
- 14 GB RAM per node
- Auto-scaling: 2-8 workers

**Recommended for Production:**
- Serverless compute (auto-scaling)
- Job compute for scheduled runs
- All-purpose only for development

### Installation

1. Upload notebook to Databricks workspace
2. Configure job with parameters:
   ```json
   {
     "domain": "Load Detail",
     "file_pattern": "",
     "dedup_strategy": "keep_all"
   }
   ```
3. Set schedule (e.g., hourly: `0 * * * *`)
4. Configure alerts on job failure

---

## Operational Procedures

### Monitoring Queries

**Check Recent Runs:**
```sql
SELECT 
    execution_id,
    file_count,
    total_rows,
    ROUND(duration_seconds, 1) as duration_sec,
    status,
    start_time
FROM dev_bronze.bronze_audit.batch_processing_audit
ORDER BY start_time DESC
LIMIT 10;
```

**Performance Analysis:**
```sql
SELECT 
    file_name,
    rows_processed,
    ROUND(rows_per_second, 0) as rows_sec,
    ROUND(duration_seconds, 2) as duration_sec
FROM dev_bronze.bronze_audit.performance_metrics
WHERE execution_id = '<execution_id>'
ORDER BY duration_seconds DESC;
```

**Cost Analysis:**
```sql
SELECT 
    DATE(created_timestamp) as date,
    COUNT(*) as files,
    SUM(rows_processed) as total_rows,
    ROUND(SUM(total_cost_usd), 4) as cost_usd,
    ROUND(AVG(cost_per_row), 8) as avg_cost_per_row
FROM dev_bronze.cost_tracking.ingestion_costs
WHERE created_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY 1
ORDER BY 1 DESC;
```

**Check for Issues:**
```sql
SELECT 
    timestamp,
    severity,
    message,
    execution_id
FROM dev_bronze.bronze_audit.notifications
WHERE severity IN ('ERROR', 'WARNING')
  AND timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY timestamp DESC;
```

### Job Exit Codes

The notebook exits with descriptive status messages for orchestration:

**Success States:**
- `SUCCESS: Processed X files, Y rows in Z.Zs` - Normal completion
- `SUCCESS: No files to process` - No new files found

**Warning States:**
- `WARNING: Processed X files but got 0 rows` - Data quality issue
- `WARNING: Processing took X seconds` - Performance degradation

**Failure States:**
- `FAILED: <error message>` - Processing failure

### Troubleshooting

**Issue:** Files not processing
**Check:**
```sql
SELECT file_name, status 
FROM dev_bronze.bronze_audit.file_processing_audit
WHERE status = 'FAILED'
ORDER BY start_time DESC;
```

**Issue:** High costs
**Check:**
```sql
SELECT 
    cluster_id,
    COUNT(*) as runs,
    ROUND(AVG(cost_per_row), 8) as avg_cost,
    ROUND(SUM(total_cost_usd), 2) as total_cost
FROM dev_bronze.cost_tracking.ingestion_costs
GROUP BY 1
ORDER BY 4 DESC;
```

**Issue:** Slow performance
**Check:**
```sql
SELECT 
    AVG(rows_per_second) as avg_throughput,
    MIN(rows_per_second) as min_throughput
FROM dev_bronze.bronze_audit.performance_metrics
WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAYS;
```

---

## Cost Optimization

### Compute Type Recommendations

**Production Workloads:**
Use **Serverless** for best cost/performance:
- $0.07 per DBU (lowest rate)
- Auto-scaling (no idle costs)
- Fast startup times
- Optimal for variable workloads

**Scheduled Batch Jobs:**
Use **Job Compute** for predictable costs:
- $0.10 per DBU + VM costs
- Right-sized for workload
- Terminates after completion
- Good for consistent daily/hourly jobs

**Development Only:**
Avoid **All-Purpose** in production:
- $0.40 per DBU (5.7x more expensive)
- Designed for interactive work
- Idle time = wasted money

### Cost Monitoring

Use included cost analysis queries (`cost_analysis_queries.sql`):
1. Cost summary by compute type
2. Daily cost trends
3. Cost efficiency by domain
4. Monthly cost projections
5. Anomaly detection

---

## Performance Tuning

### Spark Configuration

The notebook automatically configures Spark for optimal performance:
- Adaptive query execution
- Partition coalescing
- Optimized writes
- Auto-compaction
- Vectorized CSV reading

### Z-Ordering

Automatic Z-ordering on:
- `load_number`
- `customer_name`
- `current_tender_carrier`

Optimizes query performance for common access patterns.

### Partitioning

Data partitioned by `processing_date` for:
- Efficient time-range queries
- Easy data lifecycle management
- Reduced query scanning

---

## Testing

### Unit Testing

Test with sample files in `/incoming/` directory:
```bash
# Upload test files
/mnt/mg-gold-raw-files/raw_load_details/incoming/test_file_1.csv
/mnt/mg-gold-raw-files/raw_load_details/incoming/test_file_2.csv
```

Run notebook with test parameters:
- domain: Load Detail
- file_pattern: test_
- dedup_strategy: keep_all

Verify:
1. Both files processed successfully
2. Audit tables populated
3. Performance metrics recorded
4. Cost tracking complete

### Integration Testing

1. Process production-like file volumes (100+ files)
2. Verify performance meets SLAs
3. Check cost tracking accuracy
4. Validate audit trail completeness

---

## Migration from Layer 2

### Key Differences

| Aspect | Layer 2 (Serial) | Layer 3 (Parallel) |
|--------|------------------|-------------------|
| Processing | One file at a time | All files at once |
| Performance | 2 sec per file | 10-60 sec per batch |
| Audit | Per-file only | Batch + per-file |
| Cost tracking | Basic | Comprehensive |
| Compute types | Limited detection | All types supported |

### Migration Steps

1. Test Layer 3 with small file batches
2. Compare performance metrics
3. Validate audit tables match expectations
4. Review cost tracking data
5. Schedule parallel runs for validation period
6. Cut over production traffic

---

## Support

### Documentation

- This README - Overview and operations
- `cost_analysis_queries.sql` - Leadership cost queries
- `performance_optimization_guide.md` - Performance tuning
- `modularization_strategy.md` - Code architecture

### Monitoring

Monitor these key metrics:
- **Throughput:** rows_per_second > 1,000
- **Success Rate:** > 99%
- **Cost per Row:** < $0.0001
- **Duration:** P95 < 120 seconds

### Alerting

Set up alerts for:
- Job failures (severity: ERROR)
- Processing duration > 300 seconds
- Cost per row > 2x baseline
- Zero rows processed with files present

---

## Version History

### v3.0.0 - February 12, 2026 (Current)

**Features:**
- Spark parallel processing implementation
- Unity Catalog compatibility
- Enhanced compute type detection
- Comprehensive cost tracking
- Idempotency controls
- Production exit codes
- Serverless support

**Performance:**
- 10-20x faster than Layer 2
- Supports 1,000+ files per batch
- Sub-minute processing for typical workloads

**Compatibility:**
- Serverless compute
- Job compute
- All-purpose clusters
- SQL warehouses
- Unity Catalog

---

## License

Internal use only. Property of [Arkham Analytics].

---

## Contact

**Data Engineering Team**  
Email:mpwagner1@gmail.com

**Technical Lead:** [Michael Wagner]  
---

**Built for production. Optimized for performance. Designed for scale.**
