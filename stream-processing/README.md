# Stream Processing

This module implements the stream-processing component of the MES Big Data
pipeline using Apache Spark Structured Streaming.

## Current Features

The current implementation provides:

- Spark Structured Streaming
- 10-second window aggregation per machine
- Average, minimum and maximum temperature calculation
- Event count per window
- Event-time processing with watermarks
- Machine status processing
- Configurable temperature threshold
- Detection of temperature limit violations
- Spark checkpointing

## Configuration

The temperature limit can be configured using the environment variable:

`TEMP_LIMIT`

If no value is provided, the default temperature limit is `95.0`.

Example using PowerShell:

```powershell
$env:TEMP_LIMIT="95"
python .\streaming_job.py