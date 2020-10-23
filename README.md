# Kafka to HBase

## Links

1. [Application Configurations](docs/k2hb-configurations.md)
1. [Local Development](docs/local-development.md)
1. [Kafka samples and tutorial](docs/kafka-tutorial-samples.md)
1. [Agreed Schemas with upstream sources](agreed_schemas_with_upstream_sources)

## Summary

Providing a way of migrating data in Kafka topics into tables in Hbase,
preserving versions based on Kafka message timestamps.

Two columns are written to for each message received; one to store the body
of the message and one to store a count and last received date of the
topic. These are configured using the `K2HB_KAFKA_TOPIC_*` and
`K2HB_KAFKA_DATA_*` environment variables.

By default if the kafka topic is `db.database-name.collection-name` the data table is `database_name:collection_name` 
with a column family of `topic`. 
Collections support additional `.` characters but hbase does not, so from `db.database-name.coll-ection.name` we would 
get a table `database_name:coll_ection_name`

The qualifier is the topic name, the body of the cell is the raw message
received from Kafka and the version is the timestamp of the message in
milliseconds.

For example, after receiving a single message on `db.my.data` the data
is saved as follows:

```
hbase(main):001:0> scan 'my:data'
ROW                                       COLUMN+CELL
 63213667-c5a5-4411-a93b-e2da709c553e     column=topic:my:data, timestamp=1563547895682, value=<entire message body>
1 row(s) in 0.1090 seconds
```

Kafka2Hbase will attempt to create the required namespaces, tables and column families on startup - If they already exist, nothing will happen. 

By default the data table column family has a maximum of MAXINT versions (approximately 2.1 billion) and a minimum of 1 version; There is no TTL.

## Agreed Schemas with upstream sources.

Currently the only upstream source is the UC Kafka Broker.

All the schemas are found in [src/main/resources](src/main/resources)

### UC Common elements

* `$.@type`, `$.version`, and `$.timestamp` are standard wrapper elements, so they should always be present.
* All have a few root elements that are mandatory but may be null:
  * `$.unitOfWorkId` is nullable.  In practice it will always be there for the business messages, but UC don't enforce that and it wouldn't be wrong if it was null. UC use it for application-level transactions, so it's almost always present.  But there are circumstances where we do things outside of a transaction (usually for long-running batch jobs), so it's not guaranteed.
  * `$.traceId` can also be nullable, for the same reasons.  It would not be null in practice, but in terms of validation it wouldn't be invalid if it were null. UC log audit messages outside of a transaction sometimes, as those are generated on some read-only events.  It would be unusual to do so for DB-writes, but not impossible.  As for equality messages, that logic pathway is simple enough that they never write them outside of a transaction at present, but there's nothing stopping that changing in future, so it would be a better fit (in terms of business rules) if it was nullable there too.
* All require the `$.message` block.
* All require the `$.message.encryption` block, and carry the dbObject as encrypted data.
* We reject any message with a `$.message.dbObject` that looks like JSON.

### UC Business Data Schema

* For Business messages we should only assert that `$.message._id` exists, as what is in it can vary a lot per collection, in structure, type and content.
* Business messages always have `$.message.db`.
* Business messages always have `$.message.collection`.
* These are sourced from many topics, the name of which is deterministically related to the `db` and `collection`.

### UC Equality Data Schema

* Equality messages will always have exactly `$.message._id.messageId="non-zero-string"` so we can inist on `messageId` always exisiting and being >1 in length.
* Equality message do not have `$.message.db` or `$.message.collection` as this is non-specific Equality Act data like demographic spreads.
* These are sourced from a single static collection.

### UC Audit Data Schema

* Audit messages will always have exactly `$.message._id.auditId="non-zero-string"` so we can insit on it always exisiting and being >1 in length.
* Audit message do not have `$.message.db` or `$.message.collection` as this is non-specific Audit information like which user logged on to the system, or which type of update was performed.
* These are sourced from a single static collection.
