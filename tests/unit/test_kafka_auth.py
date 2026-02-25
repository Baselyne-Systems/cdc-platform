"""Unit tests for Kafka auth config builder."""

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from cdc_platform.config.models import KafkaAuthMechanism, KafkaConfig
from cdc_platform.streaming.auth import build_kafka_auth_config


class TestBuildKafkaAuthConfig:
    def test_none_mechanism_returns_empty(self):
        config = KafkaConfig()
        result = build_kafka_auth_config(config)
        assert result == {}

    def test_sasl_plain(self):
        config = KafkaConfig(
            auth_mechanism=KafkaAuthMechanism.SASL_PLAIN,
            security_protocol="SASL_SSL",
            sasl_username="user",
            sasl_password=SecretStr("pass"),
        )
        result = build_kafka_auth_config(config)
        assert result["security.protocol"] == "SASL_SSL"
        assert result["sasl.mechanism"] == "PLAIN"
        assert result["sasl.username"] == "user"
        assert result["sasl.password"] == "pass"

    def test_sasl_scram_256(self):
        config = KafkaConfig(
            auth_mechanism=KafkaAuthMechanism.SASL_SCRAM_256,
            security_protocol="SASL_SSL",
            sasl_username="user",
            sasl_password=SecretStr("pass"),
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "SCRAM-SHA-256"

    def test_sasl_scram_512(self):
        config = KafkaConfig(
            auth_mechanism=KafkaAuthMechanism.SASL_SCRAM_512,
            security_protocol="SASL_SSL",
            sasl_username="user",
            sasl_password=SecretStr("pass"),
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "SCRAM-SHA-512"

    def test_sasl_iam_sets_oauthbearer(self):
        config = KafkaConfig(
            auth_mechanism=KafkaAuthMechanism.SASL_IAM,
            security_protocol="SASL_SSL",
            aws_region="us-east-1",
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "OAUTHBEARER"
        assert callable(result["oauth_cb"])

    def test_sasl_oauthbearer_sets_oauth(self):
        config = KafkaConfig(
            auth_mechanism=KafkaAuthMechanism.SASL_OAUTHBEARER,
            security_protocol="SASL_SSL",
        )
        result = build_kafka_auth_config(config)
        assert result["sasl.mechanism"] == "OAUTHBEARER"
        assert callable(result["oauth_cb"])

    def test_ssl_locations_included(self):
        config = KafkaConfig(
            auth_mechanism=KafkaAuthMechanism.SASL_PLAIN,
            security_protocol="SASL_SSL",
            sasl_username="user",
            sasl_password=SecretStr("pass"),
            ssl_ca_location="/ca.pem",
            ssl_certificate_location="/cert.pem",
            ssl_key_location="/key.pem",
        )
        result = build_kafka_auth_config(config)
        assert result["ssl.ca.location"] == "/ca.pem"
        assert result["ssl.certificate.location"] == "/cert.pem"
        assert result["ssl.key.location"] == "/key.pem"


class TestKafkaAuthValidation:
    def test_iam_requires_aws_region(self):
        with pytest.raises(ValidationError, match="aws_region"):
            KafkaConfig(
                auth_mechanism=KafkaAuthMechanism.SASL_IAM,
            )

    def test_sasl_plain_requires_username_password(self):
        with pytest.raises(ValidationError, match="sasl_username"):
            KafkaConfig(
                auth_mechanism=KafkaAuthMechanism.SASL_PLAIN,
            )

    def test_sasl_scram_requires_username_password(self):
        with pytest.raises(ValidationError, match="sasl_username"):
            KafkaConfig(
                auth_mechanism=KafkaAuthMechanism.SASL_SCRAM_256,
            )
