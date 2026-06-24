SELECT
    execution_timestamp,
    layer,
    source_system,
    rows_read,
    rows_quarantine,
    rows_written,
    execution_time_seconds
FROM main.ifood.pipeline_audit
ORDER BY execution_timestamp DESC;