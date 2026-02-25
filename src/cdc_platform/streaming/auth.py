"""Kafka authentication config builder for managed services (MSK, GCP Managed Kafka)."""

from __future__ import annotations

from typing import Any

from cdc_platform.config.models import KafkaAuthMechanism, KafkaConfig


def build_kafka_auth_config(config: KafkaConfig) -> dict[str, Any]:
    """Build confluent_kafka config dict entries for authentication.

    Returns a dict of config keys to merge into Consumer/Producer/AdminClient
    constructor arguments.
    """
    if config.auth_mechanism == KafkaAuthMechanism.NONE:
        return {}

    auth: dict[str, Any] = {
        "security.protocol": config.security_protocol,
    }

    # SSL certificate paths
    if config.ssl_ca_location:
        auth["ssl.ca.location"] = config.ssl_ca_location
    if config.ssl_certificate_location:
        auth["ssl.certificate.location"] = config.ssl_certificate_location
    if config.ssl_key_location:
        auth["ssl.key.location"] = config.ssl_key_location

    mech = config.auth_mechanism

    if mech == KafkaAuthMechanism.SASL_PLAIN:
        auth["sasl.mechanism"] = "PLAIN"
        auth["sasl.username"] = config.sasl_username
        pw = config.sasl_password.get_secret_value() if config.sasl_password else ""
        auth["sasl.password"] = pw

    elif mech == KafkaAuthMechanism.SASL_SCRAM_256:
        auth["sasl.mechanism"] = "SCRAM-SHA-256"
        auth["sasl.username"] = config.sasl_username
        pw = config.sasl_password.get_secret_value() if config.sasl_password else ""
        auth["sasl.password"] = pw

    elif mech == KafkaAuthMechanism.SASL_SCRAM_512:
        auth["sasl.mechanism"] = "SCRAM-SHA-512"
        auth["sasl.username"] = config.sasl_username
        pw = config.sasl_password.get_secret_value() if config.sasl_password else ""
        auth["sasl.password"] = pw

    elif mech == KafkaAuthMechanism.SASL_IAM:
        auth["sasl.mechanism"] = "OAUTHBEARER"
        auth["oauth_cb"] = _build_msk_oauth_callback(config.aws_region or "us-east-1")

    elif mech == KafkaAuthMechanism.SASL_OAUTHBEARER:
        auth["sasl.mechanism"] = "OAUTHBEARER"
        auth["oauth_cb"] = _build_gcp_oauth_callback(config.gcp_project_id)

    return auth


def _build_msk_oauth_callback(region: str) -> Any:
    """Build an OAuth token callback for AWS MSK IAM authentication.

    Uses ``aws-msk-iam-sasl-signer-python`` to generate short-lived tokens.
    """

    def _msk_oauth_cb(config_str: str) -> tuple[str, float]:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms / 1000.0

    return _msk_oauth_cb


def _build_gcp_oauth_callback(project_id: str | None) -> Any:
    """Build an OAuth token callback for GCP Managed Kafka.

    Uses ``google-auth`` Application Default Credentials.
    """

    def _gcp_oauth_cb(config_str: str) -> tuple[str, float]:
        import google.auth
        import google.auth.transport.requests

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        expiry = credentials.expiry
        expiry_ts = expiry.timestamp() if expiry else 0.0
        return credentials.token, expiry_ts

    return _gcp_oauth_cb
