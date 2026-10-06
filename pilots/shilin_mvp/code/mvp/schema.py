"""Frozen table schemas and enumerations for the MVP release."""

EVENT_FIELDS = [
    "event_key", "network", "platform", "transaction_signature", "slot",
    "block_time", "outer_instruction_index", "inner_instruction_index",
    "mint", "creator", "metadata_uri", "decoder_version", "event_status",
]
PLAN_FIELDS = [
    "event_key", "checkpoint", "required", "scheduled_at", "earliest_allowed_at",
    "max_lateness_seconds", "protocol_version", "plan_status",
]
ATTEMPT_FIELDS = [
    "request_id", "event_key", "checkpoint", "uri", "request_url", "route_id", "host", "attempt",
    "started_at", "completed_at", "available_at", "status", "http_status",
    "redirect_location", "response_bytes", "response_sha256", "retry_after_seconds",
    "error_class", "error_message", "policy_state",
]
SNAPSHOT_FIELDS = [
    "request_id", "event_key", "checkpoint", "uri", "request_url", "route_id", "retrieved_at",
    "content_type", "response_sha256", "response_bytes", "parse_state", "integrity_state",
]
FIELD_FIELDS = [
    "request_id", "event_key", "checkpoint", "uri", "field_name", "json_pointer",
    "presence", "value_type", "allowed_value", "value_hash", "parser_version",
]
LEDGER_FIELDS = [
    "event_key", "checkpoint", "required", "schedule_state", "request_state",
    "parse_state", "field_state", "timing_state", "final_state", "request_id",
]

REQUIRED_CHECKPOINTS = ("T0", "T+24h", "T+60h")
ALLOWED_FIELD_NAMES = ("name", "symbol", "description", "createdOn", "image", "website", "twitter", "telegram")
ALLOWED_FIELD_TYPES = {"name": "string", "symbol": "string", "description": "string", "createdOn": "string", "image": "string", "website": "string", "twitter": "string", "telegram": "string"}
TERMINAL_REQUEST_STATUSES = {"success", "http_error", "timeout", "transport_error", "not_collected_policy", "route_refused_policy", "parse_error", "early_prohibited"}
TERMINAL_LEDGER_STATES = {"observed", "request_failed", "not_collected_policy", "parse_failed", "missed", "early_prohibited"}
