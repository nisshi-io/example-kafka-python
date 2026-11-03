# Kafka Python client with Nisshi

[Nisshi](https://nisshi.io) is an Apache Kafka®-compatible broker that stores
its data in PostgreSQL, or in S3 (experimental). This example uses the
[kafka-python](https://github.com/dpkp/kafka-python) client,
to send and receive messages with [Nisshi](https://nisshi.io).

In this example, [MinIO](https://min.io), [PostgreSQL](https://www.postgresql.org)
and [Nisshi](https://nisshi.io) run in [Docker](https://docs.docker.com/desktop/)
[Compose](https://docs.docker.com/compose/)
for easy installation and setup.

Start off by cloning this repository:

```shell
git clone https://github.com/nisshi-io/example-kafka-python.git
cd example-kafka-python
```

Copy `example.env` into `.env` so that you have a local working copy:

```shell
cp example.env .env
```

By default, Nisshi stores its data in [PostgreSQL](https://www.postgresql.org):

```
STORAGE_ENGINE="postgres://postgres:postgres@db"
```

To use S3 with [MinIO](https://min.io) instead, uncomment this line and comment
out the other `STORAGE_ENGINE` definitions in your `.env`:

```
STORAGE_ENGINE="s3://nisshi/"
```

If you're using MinIO, create its bucket before you start Nisshi, because
Nisshi checks at startup that the bucket exists. Start MinIO and wait for it
to become ready:

```shell
docker compose up -d --wait minio
docker compose exec minio /usr/bin/mc ready local
```

Then, set up some default MinIO credentials, and create a bucket for Nisshi:

```shell
docker compose exec minio /usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose exec minio /usr/bin/mc mb local/nisshi
```

Now, start up PostgreSQL and Nisshi (and MinIO, if it isn't running yet):

```shell
docker compose up -d --wait
```

Nisshi creates its tables in PostgreSQL when it first starts, so PostgreSQL
needs no extra configuration.

Using [uv](https://docs.astral.sh/uv/), sync the project:

```shell
uv sync --all-groups
```

Run the example:

```shell
uv run example.py
```

You should see the following output:

```
ConsumerRecord(topic='my-topic', partition=0, offset=0, timestamp=1740584217091, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=1, timestamp=1740584217091, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=2, timestamp=1740584218091, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=3, timestamp=1740584218092, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=4, timestamp=1740584219092, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=5, timestamp=1740584219092, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=6, timestamp=1740584220092, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=7, timestamp=1740584220092, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=8, timestamp=1740584221092, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=9, timestamp=1740584221093, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=10, timestamp=1740584222093, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=11, timestamp=1740584222093, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=12, timestamp=1740584223093, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=13, timestamp=1740584223093, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=14, timestamp=1740584224094, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=15, timestamp=1740584224094, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=16, timestamp=1740584225094, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=17, timestamp=1740584225094, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=18, timestamp=1740584226094, timestamp_type=0, key=None, value=b'test', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=4, serialized_header_size=-1)
ConsumerRecord(topic='my-topic', partition=0, offset=19, timestamp=1740584226095, timestamp_type=0, key=None, value=b'\xc2Hola, mundo!', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=13, serialized_header_size=-1)
```

In this example we have used the [kafka-python](https://github.com/dpkp/kafka-python)
client to send and receive messages to Nisshi, an Apache Kafka®-compatible broker using
PostgreSQL or S3 for storage.
