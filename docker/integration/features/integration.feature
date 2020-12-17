Feature: K2HB Integration tests

  Scenario: HBase has 10 tables
    Given HBase is up and accepting connections
    Then HBase will have 5 tables
    And each table will have 500 rows

  Scenario: The objects given to S3 should be as expected
    Given all objects can be retrieved from the ucarchive S3 bucket
    Then the total size of the retrieved data should be 5 topics with 500 records
    Then each of the objects should have the correct data

  Scenario: The manifests given to S3 should be as expected
    Given all objects can be retrieved from the manifests S3 bucket
    Then each of the objects should have the correct fields

  Scenario: The metadatastore properly reflects the state of records in HBase
    Given the metadatastore is up and accepting connections
    Then the metadatastore ucfs table will have 2500 rows
    And for each record in the ucfs table the hbase_timestamp will be populated