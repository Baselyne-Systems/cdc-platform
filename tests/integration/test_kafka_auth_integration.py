"""Integration tests for Kafka auth config builder.

Verifies that auth config flows correctly through consumer, producer,
and admin client construction for all supported auth mechanisms.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from cdc_platform.config.models import (
    KafkaAuthMechanism,
    KafkaConfig,
)
from cdc_platform.streaming.auth import build_kafka_auth_config


@pytest.mark.integration
class TestKafkaAuthConfigBuilder:
    """Verify build_kafka_auth_config produces correct configs."""

    def test_none_mechanism_returns_empty(self):
        config = KafkaConfig(auth_mechanism=KafkaAuthMechanism.NONE)
        assert build_kafka_auth_config(config) == {}

    def test_sasl_plain_config(self):
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_PLAIN,
            sasl_username="user",
            sasl_password=SecretStr("pass"),
        )
        result = build_kafka_auth_config(config)
        assert result["security.protocol"] == "SASL_SSL"
        assert result["sasl.mechanism"] == "PLAIN"
        assert result["sasl.username"] == "user"
        assert result["sasl.password"] == "pass"

    def test_sasl_scram_256_config(self):
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_SCRAM_256,
            sasl_username="user",
            sasl_password=SecretStr("pass"),
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "SCRAM-SHA-256"

    def test_sasl_scram_512_config(self):
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_SCRAM_512,
            sasl_username="user",
            sasl_password=SecretStr("pass"),
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "SCRAM-SHA-512"

    def test_sasl_iam_config(self):
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_IAM,
            aws_region="us-west-2",
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "OAUTHBEARER"
        assert callable(result["oauth_cb"])

    def test_sasl_oauthbearer_config(self):
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_OAUTHBEARER,
            gcp_project_id="my-project",
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "OAUTHBEARER"
        assert callable(result["oauth_cb"])

    def test_ssl_paths_included(self):
        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_PLAIN,
            sasl_username="u",
            sasl_password=SecretStr("p"),
            ssl_ca_location="/path/ca.pem",
            ssl_certificate_location="/path/cert.pem",
            ssl_key_location="/path/key.pem",
        )
        result = build_kafka_auth_config(config)
        assert result["ssl.ca.location"] == "/path/ca.pem"
        assert result["ssl.certificate.location"] == "/path/cert.pem"
        assert result["ssl.key.location"] == "/path/key.pem"


@pytest.mark.integration
class TestKafkaAuthValidation:
    """Verify config validation rejects invalid auth configurations."""

    def test_iam_requires_aws_region(self):
        with pytest.raises(ValueError, match="aws_region"):
            KafkaConfig(
                security_protocol="SASL_SSL",
                auth_mechanism=KafkaAuthMechanism.SASL_IAM,
            )

    def test_plain_requires_username_and_password(self):
        with pytest.raises(ValueError, match="sasl_username"):
            KafkaConfig(
                security_protocol="SASL_SSL",
                auth_mechanism=KafkaAuthMechanism.SASL_PLAIN,
            )

    def test_scram_requires_username_and_password(self):
        with pytest.raises(ValueError, match="sasl_username"):
            KafkaConfig(
                security_protocol="SASL_SSL",
                auth_mechanism=KafkaAuthMechanism.SASL_SCRAM_512,
            )


@pytest.mark.integration
class TestKafkaAuthCallbacks:
    """Verify OAuth callback functions are correctly wired."""

    def test_msk_callback_invokes_signer(self):
        import sys

        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_IAM,
            aws_region="us-east-1",
        )
        result = build_kafka_auth_config(config)
        callback = result["oauth_cb"]

        mock_provider = MagicMock()
        mock_provider.generate_auth_token.return_value = ("token123", 1000000)

        mock_module = MagicMock()
        mock_module.MSKAuthTokenProvider = mock_provider

        with patch.dict(sys.modules, {"aws_msk_iam_sasl_signer": mock_module}):
            token, expiry = callback("config_str")
            assert token == "token123"
            assert expiry == 1000.0

    def test_gcp_callback_invokes_google_auth(self):
        import sys

        config = KafkaConfig(
            security_protocol="SASL_SSL",
            auth_mechanism=KafkaAuthMechanism.SASL_OAUTHBEARER,
            gcp_project_id="test-project",
        )
        result = build_kafka_auth_config(config)
        callback = result["oauth_cb"]

        mock_creds = MagicMock()
        mock_creds.token = "gcp-token-abc"
        mock_creds.expiry = None

        mock_google_auth = MagicMock()
        mock_google_auth.default.return_value = (mock_creds, "project")

        mock_transport_requests = MagicMock()
        mock_transport = MagicMock()
        mock_transport.requests = mock_transport_requests

        mock_google = MagicMock()
        mock_google.auth = mock_google_auth
        mock_google.auth.transport = mock_transport
        mock_google.auth.transport.requests = mock_transport_requests

        with patch.dict(
            sys.modules,
            {
                "google": mock_google,
                "google.auth": mock_google_auth,
                "google.auth.transport": mock_transport,
                "google.auth.transport.requests": mock_transport_requests,
            },
        ):
            token, expiry = callback("config_str")
            assert token == "gcp-token-abc"
            assert expiry == 0.0
            mock_creds.refresh.assert_called_once()
