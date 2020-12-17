Feature: K2HB Integration tests

  Scenario: HBase has 10 tables
    Given HBase is up and accepting connections
    Then HBase will have 5 tables
    And each table will have 250 rows

  Scenario: The objects given to S3 should be as expected
    Given all objects can be retrieved from the ucarchive S3 bucket
    Then the total size of the retrieved data should be 5 topics with 250 records
    And each of the objects should have the correct data

  Scenario: The manifests given to S3 should be as expected
    Given all objects can be retrieved from the manifests S3 bucket
    Then each of the objects should have the correct fields
