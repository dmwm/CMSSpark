#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : es.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : ES API for OpenSearch and ES8+.
"""

import datetime
import json
import logging
import sys
import time

import elasticsearch


class EsInterface(object):
    """ES API for OpenSearch or ES8+

    It uses port 443 HTTPS port and url should end with /es, see `prepare_hostname` function
    """
    # Logging format
    logging_fmt = '[%(asctime)s' + time.strftime('%z') + '] [ES] [%(levelname)s] %(message)s'

    # This producer adds this reserved timestamp key to all documents
    metadata_time_field = "EsProducerTime"

    def __init__(self, es_conf, logging_level=logging.INFO, logger=None):
        """OpenSearch connection interface

        Args:
            es_conf: JSON configuration file which contains elasticsearch credentials: username, password, host

        Example conf file:
            {
                "username": "admin",
                "password": "admin",
                "hostname": "https://es-cms1.cern.ch:443/es"
            }
        """
        if logger:
            self.logger = logger
        else:
            # Prepare completely ISOLATED logger
            self.logger = logging.getLogger('es-opensearch')
            self.logger.setLevel(logging_level)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging_level)
            console_handler.setFormatter(logging.Formatter(self.logging_fmt))
            self.logger.addHandler(console_handler)

        if not es_conf:
            self.logger.error("Failed to create ElasticSearch interface, please provide es-conf parameter")
            sys.exit(1)

        try:
            with open(es_conf) as f:
                es_creds = json.load(f)
        except Exception as e:
            self.logger.error("Failed to read es_conf: %s - err: %s", es_conf, str(e))
            sys.exit(1)

        self.logger.info("Will be connected to ElasticSearch host: %s", es_creds['hostname'])
        self.logger.info("ElasticSearch py version:%s", str(elasticsearch.__version__))

        if any((key not in es_creds) for key in ["username", "password", "hostname"]):
            self.logger.error("Not all required keys are provided: username, password, hostname. Conf file: %s",
                              es_conf)
            sys.exit(1)

        hostname = self.prepare_hostname(es_creds["hostname"])
        self.logger.info("ElasticSearch host: %s", hostname)
        self.handle = elasticsearch.Elasticsearch(
            hosts=[hostname],
            http_auth=(es_creds["username"], es_creds["password"]),
            verify_certs=True,
            ca_certs="/etc/pki/tls/certs/ca-bundle.trust.crt",
        )

    @staticmethod
    def prepare_hostname(hostname):
        """Prepare hostname string for required format: https://HOST.cern.ch:443/es"""
        if "//" in hostname:
            hostname = hostname.split("//")[1]  # remove 'https' or 'http' part
        if "." in hostname:
            hostname = hostname.split(".")[0]  # remove any '.cern....' part

        hostname = "https://" + hostname + ".cern.ch:443/es"
        return hostname

    def prepare_mappings(self, int_vals=(), text_vals=(), keyword_vals=(), date_vals=(), bool_vals=(),
                         meta_data_with_types=None):
        """Returns mappings for given fields

        Args:
            int_vals: list of integer fields
            text_vals: list of text fields, no aggregation but searchable for long string
            keyword_vals: list of keyword fields used as tags with max character limit i.e. 256
            date_vals: list of date fields
            bool_vals: list of bool fields
            meta_data_with_types: metadata mapping, i.e.: {"spider_runtime": {"type": "date", "format": "epoch_millis"}}
        Returns: ES mapping JSON
        """
        props = {}
        for name in int_vals:
            props[name] = {"type": "long"}
        for name in text_vals:
            props[name] = {"type": "text", "index": "false"}
        for name in keyword_vals:
            props[name] = {"type": "keyword"}
        for name in date_vals:
            props[name] = {"type": "date", "format": "epoch_second"}
        for name in bool_vals:
            props[name] = {"type": "boolean"}
        for name in bool_vals:
            props[name]["index"] = "false"

        # Add metadata and producer time mapping.
        meta_data_with_types = meta_data_with_types or {}
        meta_data_with_types.update({self.metadata_time_field: {"type": "date", "format": "epoch_millis"}})

        props["metadata"] = {"properties": meta_data_with_types}

        dynamic_string_template = {
            "strings_as_keywords": {
                "match_mapping_type": "string",
                "mapping": {"type": "keyword", "norms": "false", "ignore_above": 1024},
            }
        }
        mappings = {"dynamic_templates": [dynamic_string_template], "properties": props}
        self.logger.debug("Mappings will be put: %s", str(mappings))
        return mappings

    @staticmethod
    def prepare_settings():
        """Return default setting JSON"""
        settings = {
            "analysis": {
                "analyzer": {
                    "analyzer_keyword": {"tokenizer": "keyword", "filter": "lowercase"}
                }
            },
            "mapping.total_fields.limit": 2000,
        }
        return settings

    def prepare_body(self, idx, data, metadata=None):
        """Prepare document body for ElasticSearch

        Args:
            idx: full index name
            data: list of dicts that data will be sent
            metadata: dict object of general metadata
        """
        metadata = metadata or {}

        # Add producer time to each document
        metadata.update({self.metadata_time_field: round(time.time() * 1000)})
        body = []
        for doc in data:
            # Action dict
            action = {"index": {"_index": idx}}

            # If document includes its own _id, put it
            if "_id" in doc:
                action["index"]["_id"] = doc["_id"]

            # If there is general metadata, send it together with the document
            if metadata:
                doc.setdefault("metadata", {}).update(metadata)

            body.append(action)
            body.append(doc)
        return body

    def parse_errors(self, result):
        """Error parser"""
        from collections import Counter
        reasons = [
            d.get("index", {}).get("error", {}).get("reason", None) for d in result["items"]
        ]
        counts = Counter([_f for _f in reasons if _f])
        n_failed = sum(counts.values())
        self.logger.error(
            "Failed to index %d documents to ES: %s"
            % (n_failed, str(counts.most_common(3)))
        )
        return n_failed

    @staticmethod
    def get_daily_index(timestamp, template="test-unittest"):
        """Creates daily index name from index template and timestamp

        Args:
            timestamp: timestamp that for the daily index
            template: index template name

        Returns: daily index name
        """
        idx = time.strftime(
            "%s-%%Y-%%m-%%d" % template,
            datetime.datetime.utcfromtimestamp(timestamp).timetuple(),
        )
        return idx

    def create_index(self, index):
        """Checks index, if not exist then creates it"""
        if not self.handle.indices.exists(index=index):
            self.handle.indices.create(index=index)
        else:
            self.logger.debug("Index already exists: %s", index)

    def delete_index(self, index):
        """Checks index, if not exist then creates it"""
        if self.handle.indices.exists(index=index):
            self.handle.indices.delete(index=index)
        else:
            self.logger.debug("No need to delete, index not exists: %s", index)

    def put_mapping_and_setting(self, index, mappings, settings=None):
        if self.handle.indices.exists(index=index):
            try:
                if settings and mappings:
                    self.handle.indices.put_mapping(mappings, index=index)
                    self.handle.indices.put_settings(settings, index=index)
                elif mappings:
                    self.handle.indices.put_mapping(mappings, index=index)
                elif settings:
                    self.handle.indices.put_settings(settings, index=index)
                else:
                    return
            except Exception as e:
                self.logger.error("Mappings and settings creation failed: %s", str(e))
        else:
            self.logger.error("Mappings and settings creation failed, index not exists: %s", index)

    def prepare_daily_index(self, index_template, **kwargs):
        """Special function to create index, settings and mappings

        Supports only one flat data, not nested data.

        Args:
            index_template:
            kwargs:
                mappings={"int_vals": ["",""], "text_vals": ["",""], etc.}
                settings={}
        """
        idx = self.get_daily_index(time.time(), template=index_template)
        if self.handle.indices.exists(idx):
            return

        # Create index
        self.handle.indices.create(idx)

        settings, mappings = None, None
        # Get settings
        if "settings" in kwargs:
            settings = kwargs["settings"]
            self.logger.debug("Settings for index: %s -, %s", idx, settings)

        # Get mappings
        if "mappings" in kwargs:
            mappings = kwargs["mappings"]
            self.logger.debug("Mappings for index: %s -, %s", idx, mappings)

            ivals, tvals, kvals, dvals, bvals = (), (), (), (), ()
            mvals = None
            for k, v in mappings.items():
                if k == "int_vals":
                    ivals = v
                elif k == "text_vals":
                    tvals = v
                elif k == "keyword_vals":
                    kvals = v
                elif k == "date_vals":
                    dvals = v
                elif k == "bool_vals":
                    bvals = v
                elif k == "meta_data_with_types":
                    mvals = v
            mappings = self.prepare_mappings(int_vals=ivals, text_vals=tvals, keyword_vals=kvals, date_vals=dvals,
                                             bool_vals=bvals, meta_data_with_types=mvals)

        # Create mapping and setting
        self.put_mapping_and_setting(index=idx, mappings=mappings, settings=settings)
        self.logger.info("Index mappings and settings are ready: %s", idx)

    @staticmethod
    def drop_nulls_in_dict(d):  # d: dict
        """Drops the dict key if the value is None

        ES mapping does not allow None values and drops the document completely.
        """
        return {k: v for k, v in d.items() if v is not None}  # dict

    @staticmethod
    def to_chunks(data, batch_size):
        length = len(data)
        for i in range(0, length, batch_size):
            yield data[i:i + batch_size]

    def post_bulk(self, index, data, metadata, is_daily_index, batch_size=100):
        """Send data

        Args:
            index: index template name if is_daily_index True, otherwise full index name
            data: list
            is_daily_index: if it is True, daily index will be created from `index`
            metadata: general metadata for documents
            batch_size: chunk size
        """
        if is_daily_index:
            idx = self.get_daily_index(time.time(), index)
        else:
            idx = index

        # Check and create daily index
        self.create_index(idx)

        data = self.prepare_body(idx, data, metadata)
        try:
            for chunk in self.to_chunks(data, batch_size):
                res = self.handle.bulk(body=chunk, index=idx, request_timeout=60)
                if res.get("errors"):
                    self.logger.error(self.parse_errors(res))
        except Exception as e:
            self.logger.error("Bulk post finished with error: %s", str(e))
