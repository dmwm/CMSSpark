## OpenSearch Client to send data

Requirements:

- opensearch-py~=2.1
- OpenSearch cluster host name.
- Secret file one line of 'username:password' of the OpenSearch. Ask to CMS Monitoring team.
- Index mapping schema and settings, see get_index_schema() below.
- Index template starting with 'test-' if you want to send to es-cms cluster "test" tenant
- `index_mod`: Let's assume you've an index_template of "test-foo". Depending on your input and provided timestamp, data
  will be sent to below indexes:
    - index_mod="": `test-foo`
    - index_mod="Y": `test-foo-YYYY`
    - index_mod="M": `test-foo-YYYY-MM`
    - index_mod="D": `test-foo-YYYY-MM-DD`

## How to use

##### Simple detailed

```python

import time
from CMSSpark.osearch import osearch

# Example documents to send
docs = [{"timestamp": "int epoch_seconds", "field1": "t1 short", "field2": "t2 long text", "count": 1}, {...}, {...},
        ...]


# Define your index mapping and settings:
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
        }
    }


_index_template = 'test-foo'
client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())

# index_mod="": 'test-foo', index_mod="Y": 'test-foo-YYYY', index_mod="M": 'test-foo-YYYY-MM', index_mod="D": 'test-foo-YYYY-MM-DD',
idx = client.get_or_create_index(timestamp=time.time(), index_template=_index_template, index_mod="")

client.send(idx, docs, metadata=None, batch_size=10000, drop_nulls=False)
```

##### Big Spark dataframe to OpenSearch, efficiently

If you're wondering how to send very big data of PySpark dataframe, here is a tip.

```python
# See "get_index_schema()" example from previous example.
# See "index_mod" explanation in the "Requirements" part.

from CMSSpark.osearch import osearch

for part in df.rdd.mapPartitions().toLocalIterator():
    # You can define below calls in a function for re-usability
    _index_template = 'test-foo'
    client = osearch.get_es_client("es-cms1.cern.ch/es", 'secret_opensearch.txt', get_index_schema())
    print(f"Length of partition: {len(part)}")
    idx = client.get_or_create_index(timestamp=part[0]['timestamp'], index_template=_index_template,
                                     index_mod="D")  # sends to daily index
    client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)
```

##### Small Spark dataframe

Go with the first example to send:

`docs = df.toPandas().to_dict('records') # and go with 'Simple detailed` example`
