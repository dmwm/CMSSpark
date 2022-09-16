#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : test-es.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : UnitTest of ES API for OpenSearch.
"""

import time
import unittest
from datetime import datetime

from es import EsInterface

es_conf = "./es_conf_test.json"


class TestEsInterface(unittest.TestCase):
    """Test EsInterface with given test credentials

    Please use test user which has only access to "test-*" indices
    """

    es = None

    # Test ES index which is authorized only to `test` user in `es-cms1` ES cluster
    index_template = "test-unittest"

    def setUp(self):
        """Connect to ES"""
        self.es = EsInterface(es_conf)

    def tearDown(self):
        """Close ES connection"""
        if self.es.handle is not None:
            self.es.handle.close()

    def test_connection(self):
        """Test connection"""
        self.assertEqual(self.es.handle.ping(), True)

    def test_get_daily_index(self):
        ts_2022_1_1 = datetime(2022, 1, 1).timestamp()
        idx = self.es.get_daily_index(timestamp=ts_2022_1_1, template=self.index_template)
        self.assertEqual("test-unittest-2022-01-01", idx)

    def test_prepare_body(self):
        a = [{'index': {'_index': 'test-unittest-2022-01-01'}},
             {'v1': 1, 'metadata': {'host': 'localhost'}},
             {'index': {'_index': 'test-unittest-2022-01-01'}},
             {'v2': 2, 'metadata': {'host': 'localhost'}}]
        self.assertEqual(
            a,
            self.es.prepare_body(idx="test-unittest-2022-01-01",
                                 data=[{'v1': 1}, {'v2': 2}],
                                 metadata={'host': 'localhost'})
        )

    def test_prepare_mappings(self):
        m = self.es.prepare_mappings(
            int_vals=["intfield"],
            text_vals=["textfield"],
            keyword_vals=["keywordfield"],
            date_vals=["datefield"],
            bool_vals=["boolfield"],
            meta_data_with_types={"spider_runtime": {"type": "date", "format": "epoch_millis"}})

        expected_mapping = {
            'dynamic_templates':
                [{'strings_as_keywords': {
                    'match_mapping_type': 'string',
                    'mapping': {
                        'type': 'keyword',
                        'norms': 'false',
                        'ignore_above': 1024}
                }
                }],
            'properties': {'intfield': {'type': 'long'},
                           'textfield': {'type': 'text', 'index': 'false'},
                           'keywordfield': {'type': 'keyword'},
                           'atefield': {'type': 'date', 'format': 'epoch_second'},
                           'boolfield': {'type': 'boolean', 'index': 'false'},
                           'metadata': {'properties': {'spider_runtime': {'type': 'date', 'format': 'epoch_millis'}}}
                           }
        }
        self.assertEqual(m, expected_mapping)

    def test_create_index(self):
        idx = "1991-01-01"
        self.es.create_index(index=idx)
        self.assertEqual(self.es.handle.indices.exists(idx), True)

    def test_delete_index(self):
        idx = "1991-01-01"
        self.es.delete_index(index=idx)
        self.assertEqual(self.es.handle.indices.exists(idx), False)

    def test_post_bulk(self):
        """In the first index creation, it will prepare mapping

        !!! - If we use index template, we should not put mappings/settings parameters - !!!
        """
        mock_multiple_docs = [{"v1": 1}, {"v2": 2}]
        mock_int_vals = ["v1", "v2"]
        self.es.post_bulk(index=self.index_template,
                          data=mock_multiple_docs,
                          metadata={"timestamp": time.time()},
                          is_daily_index=True,
                          mappings=mock_int_vals,
                          settings=self.es.prepare_settings()
                          )
        """
            One of the sent bulk data in ES:
            {
              "_index": "test-unittest-2022-09-15",
              "_type": "_doc",
              "_id": "IzgTQ4MBFrD6ffPYigxs",
              "_version": 1,
              "_score": null,
              "_source": {
                "v1": 1,
                "metadata": {
                  "timestamp": 1663277697.3059578,
                  "EsProducerTime": 1663277697644
                }
              },
              "fields": {
                "metadata.EsProducerTime": [
                  "2022-09-15T21:34:57.644Z"
                ]
              },
              "sort": [
                1663277697644
              ]
            }
        """


if __name__ == '__main__':
    unittest.main()
