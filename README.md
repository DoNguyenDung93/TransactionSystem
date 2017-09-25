# TransactionSystem

### Getting started

Set up the database with `./setup.sh`


Configure these settings in the `cassandra.yaml`

```
# How long the coordinator should wait for read operations to complete
read_request_timeout_in_ms: 30000
# How long the coordinator should wait for seq or index scans to complete
range_request_timeout_in_ms: 30000
# How long the coordinator should wait for writes to complete
write_request_timeout_in_ms: 30000
# How long the coordinator should wait for counter writes to complete
counter_write_request_timeout_in_ms: 30000
# How long a coordinator should continue to retry a CAS operation
# that contends with other proposals for the same row
cas_contention_timeout_in_ms: 30000
# How long the coordinator should wait for truncates to complete
# (This can be much longer, because unless auto_snapshot is disabled
# we need to flush first so we can snapshot before removing the data.)
truncate_request_timeout_in_ms: 60000
# The default timeout for other, miscellaneous operations
request_timeout_in_ms: 30000
```
