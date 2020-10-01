# Sample 101 Kafka command lines for practice

## See also

| **Topic** | **Type** | **Link to Document** |
| --------- | -------- | -------------------- |
| K2hb: Kafka, Broker and Consumer, Kotlin | Pubic Git Repo | https://github.com/dwp/kafka-to-hbase.git  | 
| Reconciliation, Kotlin | Public Git Repo | https://github.com/dwp/kafka-to-hbase-reconciliation.git  | 
| Kafka | All-About | https://www.tutorialspoint.com/apache_kafka/apache_kafka_fundamentals.htm  | 
| Kafka Command Line | Tutorial | https://docs.cloudera.com/documentation/kafka/latest/topics/kafka_command_line.html  | 

## Begin!

### Bring up all the service containers and get a shell in the kafka box

   ```shell script
   make services
   # or
   make integration-all
   ```

then 

   ```shell script
   make kafka-shell
   # or
   docker exec -it kafka sh
   ```


### Inside the shell, find all the utility scripts

   ```shell script
   cd /opt/kafka/bin
   ls
   ```

### Check the current list of topics

   ```shell script
   ./kafka-topics.sh --zookeeper zookeeper:2181 --list
   ```

### Make a new topic

...note that doing it this way we must specify the partitions, while through code it is defaulted at the server level.

   ```shell script
   ./kafka-topics.sh --create --topic my-topic --zookeeper zookeeper:2181 --replication-factor 1 --partitions 20
   ```

or if it might already exist

   ```shell script
   ./kafka-topics.sh --if-not-exists --create --topic my-topic --zookeeper zookeeper:2181 --replication-factor 1 --partitions 20
   ```

### Describe the new topic
   ```shell script
   ./kafka-topics.sh --describe --topic my-topic --zookeeper zookeeper:2181
   ```

### Publish to Topic 

This starts an interactive prompt, these are separated by you hitting Return

...note that this interacts with the Broker rather than going through it to ZooKeeper

   ```shell script
   ./kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic
   ```

### Subscribe to Topic

...note that this interacts with the Broker rather than going through it to ZooKeeper

   ```shell script
   ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning --group my-consumer-group
   ```
