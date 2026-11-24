"""kafka_bounds against a real broker (run-kafka-it.sh): records with known timestamps, a committed group offset,
then the lookup. Not collected by pytest (no test_ prefix): it needs the broker container."""
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from lakehouse_jobs import reconcile

T0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=3)
spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
jvm = spark.sparkContext._jvm
props = {"bootstrap.servers": "kafka-it:9092"}

conf = jvm.java.util.Properties()
conf.setProperty("bootstrap.servers", props["bootstrap.servers"])
admin = jvm.org.apache.kafka.clients.admin.Admin.create(conf)
topic = jvm.org.apache.kafka.clients.admin.NewTopic("events.raw", 3, jvm.java.lang.Short.parseShort("1"))
admin.createTopics(jvm.java.util.Collections.singletonList(topic)).all().get()

conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
producer = jvm.org.apache.kafka.clients.producer.KafkaProducer(conf)
Long = jvm.java.lang.Long
def send(p, minutes):
    ts = Long.valueOf(int((T0 + timedelta(minutes=minutes)).timestamp() * 1000))
    rec = jvm.org.apache.kafka.clients.producer.ProducerRecord("events.raw", jvm.java.lang.Integer.valueOf(p), ts, "k", "v")
    producer.send(rec).get()
# Partition 0: offsets 0-1 before the window, 2-5 inside, 6-7 after. Partition 1: all inside. Partition 2: empty.
for m in (-30, -20, 5, 10, 20, 50, 70, 80):
    send(0, m)
for m in (1, 2, 3):
    send(1, m)
producer.close()

# Flink's group: committed 5 on partition 0 (behind), nothing on partition 1.
offsets = jvm.java.util.HashMap()
offsets.put(jvm.org.apache.kafka.common.TopicPartition("events.raw", 0),
            jvm.org.apache.kafka.clients.consumer.OffsetAndMetadata(5))
admin.alterConsumerGroupOffsets("flink-events-to-iceberg", offsets).all().get()
admin.close()

start, end = T0, T0 + timedelta(minutes=60)
bounds = reconcile.kafka_bounds(spark, props, "events.raw", "flink-events-to-iceberg", start, end)
print("bounds", bounds)
expected = {0: (2, 6, 5), 1: (0, 3, 0), 2: (0, 0, 0)}
assert bounds == expected, (bounds, expected)
ranges, capped = reconcile.offset_ranges(bounds)
print("ranges", ranges, "capped", capped)
assert ranges == [(0, 2, 4)] and capped == 2
print("KAFKA_BOUNDS_IT OK")
