CREATE USER IF NOT EXISTS k2hbwriter;
CREATE USER IF NOT EXISTS reconciler;
CREATE USER IF NOT EXISTS datareader;

GRANT SELECT, INSERT ON `committed_records` to k2hbwriter;
GRANT SELECT, UPDATE (reconciled_result, reconciled_timestamp) ON `committed_records` to reconciler;
GRANT SELECT ON `committed_records` to datareader;
