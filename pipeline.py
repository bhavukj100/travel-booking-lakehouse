"""Synthetic booking events to a local Bronze/Silver/Gold Parquet lakehouse."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession, Window, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField('event_id', T.StringType()), T.StructField('booking_id', T.StringType()),
    T.StructField('origin', T.StringType()), T.StructField('destination', T.StringType()),
    T.StructField('status', T.StringType()), T.StructField('amount', T.DoubleType()),
    T.StructField('event_time', T.StringType()), T.StructField('ingested_at', T.StringType())])

def transform(raw):
    parsed = raw.withColumn('event_ts', F.to_timestamp('event_time')).withColumn('ingested_ts', F.to_timestamp('ingested_at'))
    valid = (F.col('event_id').isNotNull() & F.col('booking_id').isNotNull()
             & F.col('origin').rlike('^[A-Z]{3}$') & F.col('destination').rlike('^[A-Z]{3}$')
             & (F.col('origin') != F.col('destination')) & F.col('status').isin('confirmed', 'cancelled')
             & F.col('amount').isNotNull() & ~F.isnan('amount') & (F.col('amount') >= 0)
             & (F.col('amount') < float('inf')) & F.col('event_ts').isNotNull() & F.col('ingested_ts').isNotNull())
    parsed = parsed.withColumn('is_valid', F.coalesce(valid, F.lit(False)))
    accepted = parsed.filter('is_valid').drop('is_valid')
    quarantine = parsed.filter('NOT is_valid').drop('is_valid').withColumn('reason', F.lit('invalid booking event'))
    # Retain the latest ingestion of an event, with deterministic content tie breaking.
    digest = F.sha2(F.to_json(F.struct(*accepted.columns)), 256)
    rank = Window.partitionBy('event_id').orderBy(F.desc('ingested_ts'), digest.desc())
    silver = accepted.withColumn('_rank', F.row_number().over(rank)).filter('_rank = 1').drop('_rank')
    latest = Window.partitionBy('booking_id').orderBy(F.desc('event_ts'), F.desc('ingested_ts'), F.desc('event_id'))
    state = silver.withColumn('_rank', F.row_number().over(latest)).filter('_rank = 1').drop('_rank')
    # Revenue reflects current confirmed bookings, not all historical events.
    gold = state.withColumn('booking_date', F.to_date('event_ts')).groupBy('booking_date', 'origin', 'destination').agg(
        F.count('*').alias('bookings'),
        F.sum(F.when(F.col('status') == 'confirmed', 1).otherwise(0)).alias('confirmed_bookings'),
        F.round(F.sum(F.when(F.col('status') == 'confirmed', F.col('amount')).otherwise(0)), 2).alias('confirmed_revenue'),
        F.sum(F.when(F.col('status') == 'cancelled', 1).otherwise(0)).alias('cancelled_bookings'))
    return silver, quarantine, gold

def run(spark, source, output):
    output = Path(output)
    raw = spark.read.schema(SCHEMA).json(str(source))
    raw.write.mode('overwrite').parquet(str(output / 'bronze'))
    silver, quarantine, gold = transform(raw)
    for name, frame in [('silver', silver), ('quarantine', quarantine), ('gold', gold)]:
        frame.write.mode('overwrite').parquet(str(output / name))
    report = {'input_rows': raw.count(), 'silver_rows': silver.count(), 'quarantined_rows': quarantine.count(),
              'duplicates_removed': raw.count() - quarantine.count() - silver.count()}
    output.mkdir(parents=True, exist_ok=True)
    (output / 'quality.json').write_text(json.dumps(report, indent=2) + '\n')
    gold.orderBy('booking_date', 'origin', 'destination').show(truncate=False)
    print(json.dumps(report, indent=2))
    return report

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='bookings.jsonl')
    parser.add_argument('--output', default='output')
    args = parser.parse_args()
    spark = SparkSession.builder.master('local[2]').appName('TravelLakehouse').config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        run(spark, args.input, args.output)
    finally:
        spark.stop()
