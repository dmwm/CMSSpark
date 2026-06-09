#!/usr/bin/env python
"""
OpenTelemetry setup for CMS Monitoring Spark workloads.

Sends metrics, traces, and logs to the in-cluster OpenTelemetry Collector
(opentelemetry-collector.opentelemetry.svc.cluster.local:4317 by default).

Enable by setting OTEL_ENABLED=true and OTEL_SERVICE_NAME in the workload env.
Child images built on cmsmon-spark can import:

    from helpers.otel_setup import global_meter, global_tracer, trace_span, setup_opentelemetry
"""

from __future__ import annotations

import atexit
import base64
import contextvars
import functools
import logging
import os
import re
import uuid
from typing import Callable, Dict, Optional, Tuple, TypeVar

import opentelemetry
from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Meter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode, Tracer, format_trace_id

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)

DEFAULT_COLLECTOR_ENDPOINT = (
    "opentelemetry-collector.opentelemetry.svc.cluster.local:4317"
)

EXECUTION_ID = str(uuid.uuid4())
_root_span_context = contextvars.ContextVar("root_span_context", default=None)

_otel_initialized = False


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def otel_config() -> dict:
    """Return OpenTelemetry settings from environment variables."""
    return {
        "enabled": _env_bool("OTEL_ENABLED", default=False),
        "endpoint": os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", DEFAULT_COLLECTOR_ENDPOINT),
        "metric_export_interval_ms": int(
            os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")
        ),
        "service_name": os.getenv("OTEL_SERVICE_NAME", "cmsmon-spark"),
        "service_version": os.getenv(
            "OTEL_SERVICE_VERSION",
            os.getenv("CMSMON_TAG", os.getenv("CMSSPARK_TAG", "unknown")),
        ),
        "username": os.getenv("OPENTELEMETRY_USERNAME"),
        "password": os.getenv("OPENTELEMETRY_PASSWORD"),
        "metrics_enabled": _env_bool("OTEL_METRICS_ENABLED", default=True),
        "traces_enabled": _env_bool("OTEL_TRACES_ENABLED", default=True),
        "logs_enabled": _env_bool("OTEL_LOGS_ENABLED", default=True),
        "log_level": os.getenv("OTEL_LOG_LEVEL", "INFO").upper(),
        "export_timeout_sec": int(os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT", "10")),
        "insecure": _env_bool("OTEL_EXPORTER_OTLP_INSECURE", default=True),
        "yarn_logs_enabled": _env_bool("OTEL_YARN_LOGS_ENABLED", default=True),
        "suppress_stomp_logs": _env_bool("OTEL_LOG_SUPPRESS_STOMP", default=True),
        "log_logger_min_levels": os.getenv("OTEL_LOG_LOGGER_MIN_LEVELS", ""),
    }


_DEFAULT_STOMP_LOGGER_MIN_LEVELS = {
    "stomp.py": logging.WARNING,
    "StompAMQ": logging.WARNING,
    "StompyListener": logging.WARNING,
}


_YARN_LEVEL = re.compile(r"\s(INFO|WARN|WARNING|ERROR|FATAL|DEBUG|TRACE)\s")
_YARN_BRACKET_LEVEL = re.compile(r"\[(INFO|WARN|WARNING|ERROR|FATAL|DEBUG|TRACE)\]")
_STOMP_YARN_LINE = re.compile(r"\b(stomp\.py|StompAMQ|StompAMQ7\.py|StompyListener)\b")

_YARN_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "TRACE": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
}


def get_execution_id() -> str:
    """Return the current trace ID, or the process execution ID as fallback."""
    span_context = trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        return format_trace_id(span_context.trace_id)
    return EXECUTION_ID


def get_root_span_context():
    """Return the root span context for the current execution context, if any."""
    return _root_span_context.get()


class CustomLoggingHandler(LoggingHandler):
    """LoggingHandler that adds stable service and execution attributes."""

    def __init__(self, service_name: str, service_version: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._service_name = service_name
        self._service_version = service_version

    def _get_attributes(self, record: logging.LogRecord):
        attributes = super()._get_attributes(record)
        attributes["service.name"] = self._service_name
        attributes["service.version"] = self._service_version
        attributes["execution.id"] = get_execution_id()
        return attributes


class _LoggerMinLevelFilter(logging.Filter):
    """Drop records below a configured minimum level for matching logger names."""

    def __init__(self, min_levels: Dict[str, int]):
        super().__init__()
        self._min_levels = min_levels

    def filter(self, record: logging.LogRecord) -> bool:
        for logger_name, min_level in self._min_levels.items():
            if record.name == logger_name or record.name.startswith(f"{logger_name}."):
                return record.levelno >= min_level
        return True


def _parse_logger_min_levels(raw_value: str) -> Dict[str, int]:
    """Parse OTEL_LOG_LOGGER_MIN_LEVELS, e.g. 'stomp.py:WARNING,StompAMQ:ERROR'."""
    min_levels: Dict[str, int] = {}
    for item in raw_value.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        logger_name, level_name = item.split(":", 1)
        logger_name = logger_name.strip()
        level_name = level_name.strip().upper()
        if logger_name and hasattr(logging, level_name):
            min_levels[logger_name] = getattr(logging, level_name)
    return min_levels


def _otel_logger_min_levels(config: dict) -> Dict[str, int]:
    min_levels: Dict[str, int] = {}
    if config["suppress_stomp_logs"]:
        min_levels.update(_DEFAULT_STOMP_LOGGER_MIN_LEVELS)
    min_levels.update(_parse_logger_min_levels(config["log_logger_min_levels"]))
    return min_levels


class _StompMessageFilter(logging.Filter):
    """Drop StompAMQ/stomp.py messages below WARNING from OTEL export."""

    def filter(self, record: logging.LogRecord) -> bool:
        target = f"{record.name} {record.getMessage()}"
        if _STOMP_YARN_LINE.search(target):
            return record.levelno >= logging.WARNING
        return True


def _attach_otel_log_filters(handler: logging.Handler, config: dict) -> None:
    min_levels = _otel_logger_min_levels(config)
    if min_levels:
        handler.addFilter(_LoggerMinLevelFilter(min_levels))
    if config["suppress_stomp_logs"]:
        handler.addFilter(_StompMessageFilter())


def _grpc_channel_options() -> tuple:
    return (
        ("grpc.keepalive_time_ms", 20000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", False),
        ("grpc.http2.min_time_between_pings_ms", 30000),
    )


def _grpc_endpoint(endpoint: str) -> str:
    """Return host:port for OTLP/gRPC exporters.

    Spider-style configs append /v1/{signal} to OTEL_EXPORTER_OTLP_ENDPOINT.
    gRPC exporters expect host:port only; the SDK adds signal routing internally.
    """
    endpoint = endpoint.rstrip("/")
    for suffix in ("/v1/logs", "/v1/traces", "/v1/metrics"):
        if endpoint.endswith(suffix):
            endpoint = endpoint[: -len(suffix)]
    return endpoint.rstrip("/")


def _build_otlp_headers(config: dict) -> Optional[dict]:
    username = config["username"]
    password = config["password"]
    if not username or not password:
        return None
    creds = f"{username}:{password}".encode("utf-8")
    token = base64.b64encode(creds).decode("utf-8")
    return {"authorization": f"Basic {token}"}


def setup_opentelemetry(force: bool = False) -> Tuple[Meter, Tracer]:
    """
    Initialize OpenTelemetry exporters for metrics, traces, and logs.

    Returns no-op meter and tracer instances when OTEL_ENABLED is false.
    """
    global _otel_initialized

    config = otel_config()
    meter = opentelemetry.metrics.get_meter(__name__)
    tracer = opentelemetry.trace.get_tracer(__name__)

    if not config["enabled"]:
        return meter, tracer

    if _otel_initialized and not force:
        return meter, tracer

    resource = Resource.create(
        {
            "service.name": config["service_name"],
            "service.version": config["service_version"],
            "execution.id": EXECUTION_ID,
        }
    )

    grpc_endpoint = _grpc_endpoint(config["endpoint"])
    exporter_kwargs = {
        "headers": _build_otlp_headers(config),
        "channel_options": _grpc_channel_options(),
        "insecure": config["insecure"],
        "timeout": config["export_timeout_sec"],
    }

    if config["metrics_enabled"]:
        metric_exporter = OTLPMetricExporter(
            endpoint=grpc_endpoint,
            **exporter_kwargs,
        )
        metric_reader = PeriodicExportingMetricReader(
            exporter=metric_exporter,
            export_interval_millis=config["metric_export_interval_ms"],
        )
        provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        opentelemetry.metrics.set_meter_provider(provider)
        meter = opentelemetry.metrics.get_meter(__name__)

    if config["traces_enabled"]:
        trace_exporter = OTLPSpanExporter(
            endpoint=grpc_endpoint,
            **exporter_kwargs,
        )
        trace_provider = TracerProvider(resource=resource)
        trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
        opentelemetry.trace.set_tracer_provider(trace_provider)
        tracer = opentelemetry.trace.get_tracer(__name__)

    if config["logs_enabled"]:
        logger_provider = LoggerProvider(resource=resource)
        set_logger_provider(logger_provider)

        log_exporter = OTLPLogExporter(
            endpoint=grpc_endpoint,
            **exporter_kwargs,
        )
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

        log_level = getattr(logging, config["log_level"], logging.INFO)
        root_logger = logging.getLogger()
        otel_handler = CustomLoggingHandler(
            service_name=config["service_name"],
            service_version=config["service_version"],
            logger_provider=logger_provider,
            level=log_level,
        )
        _attach_otel_log_filters(otel_handler, config)
        root_logger.addHandler(otel_handler)
        root_logger.setLevel(log_level)

        yarn_logger = logging.getLogger("cmsmon.yarn")
        yarn_logger.setLevel(logging.DEBUG)
        yarn_logger.propagate = False
        yarn_otel_handler = CustomLoggingHandler(
            service_name=config["service_name"],
            service_version=config["service_version"],
            logger_provider=logger_provider,
            level=logging.DEBUG,
        )
        _attach_otel_log_filters(yarn_otel_handler, config)
        yarn_logger.addHandler(yarn_otel_handler)

        atexit.register(_shutdown_on_exit)

    _otel_initialized = True
    logger.warning(
        "OpenTelemetry initialized: endpoint=%s, service=%s, version=%s, execution_id=%s",
        grpc_endpoint,
        config["service_name"],
        config["service_version"],
        EXECUTION_ID,
    )

    return meter, tracer


def _yarn_line_level(line: str) -> int:
    """Map a Spark/YARN or Python logging line to a Python logging level."""
    match = _YARN_LEVEL.search(line) or _YARN_BRACKET_LEVEL.search(line)
    if not match:
        return logging.INFO
    return _YARN_LEVEL_MAP.get(match.group(1), logging.INFO)


def _is_suppressed_stomp_yarn_line(line: str, config: dict) -> bool:
    if not config["suppress_stomp_logs"]:
        return False
    if not _STOMP_YARN_LINE.search(line):
        return False
    return _yarn_line_level(line) < logging.WARNING


def should_export_yarn_line(line: str) -> bool:
    """Return True when a spark-submit line should be exported and shown on stdout."""
    config = otel_config()
    message = line.rstrip()
    if not message:
        return False
    if not config["enabled"] or not config["yarn_logs_enabled"]:
        return True
    if _is_suppressed_stomp_yarn_line(line, config):
        return False
    log_level = getattr(logging, config["log_level"], logging.INFO)
    return _yarn_line_level(line) >= log_level


def log_yarn_line(line: str) -> None:
    """Send a Spark/YARN log line to OpenTelemetry."""
    if not should_export_yarn_line(line):
        return
    message = line.rstrip()
    logging.getLogger("cmsmon.yarn").log(
        _yarn_line_level(line),
        message,
        extra={"log.source": "yarn"},
    )


def shutdown_opentelemetry(timeout_millis: int = 30000) -> None:
    """Flush and shut down OpenTelemetry providers (call before short-lived jobs exit)."""
    if not _otel_initialized:
        return

    config = otel_config()
    if config["logs_enabled"]:
        from opentelemetry._logs import get_logger_provider

        provider = get_logger_provider()
        if provider is not None:
            provider.force_flush(timeout_millis=timeout_millis)
            provider.shutdown()

    if config["metrics_enabled"]:
        from opentelemetry.metrics import get_meter_provider

        provider = get_meter_provider()
        if provider is not None:
            provider.force_flush(timeout_millis=timeout_millis)
            provider.shutdown()

    if config["traces_enabled"]:
        from opentelemetry.trace import get_tracer_provider

        provider = get_tracer_provider()
        if provider is not None:
            provider.force_flush(timeout_millis=timeout_millis)
            provider.shutdown()


def _shutdown_on_exit() -> None:
    shutdown_opentelemetry()


def trace_span(span_name: Optional[str] = None, **attributes: object) -> Callable[[F], F]:
    """
    Decorator that creates an OpenTelemetry span around a function call.

    When OTEL is disabled, the wrapped function runs unchanged.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not otel_config()["enabled"]:
                return func(*args, **kwargs)

            config = otel_config()
            name = span_name or f"{config['service_name']}.{func.__module__}.{func.__name__}"
            parent_span_context = trace.get_current_span().get_span_context()
            is_root_span = not (parent_span_context and parent_span_context.is_valid)
            root_token = None

            with global_tracer.start_as_current_span(name) as span:
                if is_root_span:
                    root_token = _root_span_context.set(span.get_span_context())
                for key, value in attributes.items():
                    span.set_attribute(key, value)
                span.set_attribute("execution.id", get_execution_id())
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as exc:
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    span.record_exception(exc)
                    raise
                finally:
                    if root_token is not None:
                        _root_span_context.reset(root_token)

        return wrapper  # type: ignore[return-value]

    return decorator


global_meter, global_tracer = setup_opentelemetry()
