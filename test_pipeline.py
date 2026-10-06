import tempfile
import unittest
from pathlib import Path
from pyspark.sql import SparkSession
from pipeline import run, transform, SCHEMA

class LakehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('Tests').config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def test_quality_current_state_and_idempotency(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = run(self.spark, 'bookings.jsonl', tmp)
            self.assertEqual(report, {'input_rows': 8, 'silver_rows': 5, 'quarantined_rows': 2, 'duplicates_removed': 1})
            rows = {r.origin: r.asDict() for r in self.spark.read.parquet(str(Path(tmp) / 'gold')).collect()}
            self.assertEqual(rows['ATL']['confirmed_revenue'], 250)
            self.assertEqual(rows['ATL']['cancelled_bookings'], 1)
            self.assertEqual(rows['SFO']['confirmed_revenue'], 180)
            self.assertEqual(run(self.spark, 'bookings.jsonl', tmp), report)
    def test_missing_keys_and_invalid_status(self):
        raw = self.spark.createDataFrame([(None, 'b', 'ATL', 'JFK', 'confirmed', 10.0, '2026-01-01T10:00:00', '2026-01-01T10:00:00'), ('x', 'b', 'ATL', 'JFK', 'unknown', 10.0, '2026-01-01T10:00:00', '2026-01-01T10:00:00')], SCHEMA)
        silver, quarantine, gold = transform(raw)
        self.assertEqual(silver.count(), 0)
        self.assertEqual(quarantine.count(), 2)
        self.assertEqual(gold.count(), 0)

if __name__ == '__main__':
    unittest.main()
