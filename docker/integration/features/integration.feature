Feature: K2HB Integration tests

  Scenario: HBase has 10 tables
    Given HBase is up and accepting connections
    Then HBase will have 10 tables
    And each table will have 1000 rows

  Scenario: The objects given to S3 should be as expected
    Given all objects can be retrieved from S3
    Then the total size of the retrieved data should be 10 topics * 1000 records
    And each of the objects should have the correct data
