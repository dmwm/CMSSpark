# OpenSearch Client

## Requirements

- `opensearch-py~=2.1`
- Reachable OpenSearch cluster host
- Secret file containing `username:password`
- Index schema returned by `get_index_schema()`
- Index template (prefix with `test-` to target the os-cms “test” tenant)
- `index_mod` controls time-based suffixes for a template such as `test-foo`:
  - `""` → `test-foo`
  - `"Y"` → `test-foo-YYYY`
  - `"M"` → `test-foo-YYYY-MM`
  - `"D"` → `test-foo-YYYY-MM-DD`

## Usage

### Basic example

```python
import time
from CMSSpark.osearch import osearch

docs = [
    {"timestamp": "int epoch_seconds", "field1": "t1 short", "field2": "t2 long text", "count": 1},
    # ...
]

def get_index_schema():
    return {
        "settings": {"index": {"number_of_shards": "1", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                "timestamp": {"format": "epoch_second", "type": "date"},
                "field1": {"ignore_above": 1024, "type": "keyword"},
                "field2": {"type": "text"},
                "count": {"type": "long"},
            }
        },
    }

client = osearch.get_es_client("os-cms.cern.ch/os", "secret_opensearch.txt", get_index_schema())
idx = client.get_or_create_index(timestamp=time.time(), index_template="test-foo", index_mod="")
client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)
```

### Large Spark DataFrame

```python
from CMSSpark.osearch import osearch

for part in df.rdd.mapPartitions().toLocalIterator():
    client = osearch.get_es_client("os-cms.cern.ch/os", "secret_opensearch.txt", get_index_schema())
    idx = client.get_or_create_index(timestamp=part[0]["timestamp"], index_template="test-foo", index_mod="D")
    client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)
```

### Small Spark DataFrame

```python
docs = df.toPandas().to_dict("records")
# reuse the Basic Example
```
