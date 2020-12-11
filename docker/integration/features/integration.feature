Feature: Needs a name

  Scenario: HBase has 10 tables
    Given HBase is up and accepting connections
    Then HBase will have 10 tables
    And each table will have 1000 rows
