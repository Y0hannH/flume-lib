import pytest

from flume_lib.validation import ConfigError, validate_config

BASE = {
    "base_url": "https://api.test/items",
    "target_schema": "bronze",
    "target_table": "items",
}


def cfg(**overrides):
    return {**BASE, **overrides}


class TestTopLevel:
    def test_minimal_config_is_valid(self):
        validate_config(cfg())

    @pytest.mark.parametrize("key", ["base_url", "target_schema", "target_table"])
    def test_missing_required_raises(self, key):
        config = cfg()
        del config[key]
        with pytest.raises(ConfigError, match=key):
            validate_config(config)

    def test_unknown_key_raises(self):
        with pytest.raises(ConfigError, match="clé inconnue 'foo'"):
            validate_config(cfg(foo=1))

    def test_typo_suggests_correct_key(self):
        # le bug silencieux d'origine : 'pagintaion' ignoré = appel unique
        with pytest.raises(ConfigError, match="vouliez-vous dire 'pagination'"):
            validate_config(cfg(pagintaion={"type": "offset"}))

    def test_body_without_post_raises(self):
        with pytest.raises(ConfigError, match="method"):
            validate_config(cfg(body={"q": 1}))

    def test_unsupported_method_raises(self):
        with pytest.raises(ConfigError, match="DELETE"):
            validate_config(cfg(method="DELETE"))

    def test_not_a_dict_raises(self):
        with pytest.raises(ConfigError):
            validate_config([1, 2])


class TestAuthSection:
    def test_known_types_accepted(self):
        validate_config(cfg(auth={"type": "bearer_token", "token": {"env_var": "T"}}))
        validate_config(cfg(auth={"type": "api_key_header", "key_env_var": "K"}))
        validate_config(
            cfg(auth={"type": "basic", "username": "u", "password": {"env_var": "P"}})
        )
        validate_config(
            cfg(
                auth={
                    "type": "oauth2_client_credentials",
                    "tenant_id": "t",
                    "client_id": "c",
                    "client_secret": {"env_var": "S"},
                }
            )
        )
        validate_config(
            cfg(auth={"type": "token_endpoint", "token_url": "https://x/login"})
        )

    def test_unknown_auth_type_raises(self):
        with pytest.raises(ConfigError, match="type inconnu 'jwt'"):
            validate_config(cfg(auth={"type": "jwt"}))

    def test_unknown_auth_key_raises(self):
        with pytest.raises(ConfigError, match="clé inconnue 'tokenn'"):
            validate_config(cfg(auth={"type": "bearer_token", "tokenn": "x"}))

    def test_missing_credential_raises(self):
        with pytest.raises(ConfigError, match="'token'"):
            validate_config(cfg(auth={"type": "bearer_token", "header_name": "X"}))

    def test_oauth2_without_tenant_or_url_raises(self):
        with pytest.raises(ConfigError, match="token_url"):
            validate_config(
                cfg(auth={"type": "oauth2_client_credentials", "client_id": "c", "client_secret": "s"})
            )

    def test_secret_ref_in_get_body_raises(self):
        with pytest.raises(ConfigError, match="GET"):
            validate_config(
                cfg(
                    auth={
                        "type": "token_endpoint",
                        "token_url": "https://x/token",
                        "method": "GET",
                        "body": {"key": {"env_var": "K"}},
                    }
                )
            )


class TestPaginationSection:
    def test_known_types_accepted(self):
        validate_config(cfg(pagination={"type": "offset", "limit": 50}))
        validate_config(cfg(pagination={"type": "page", "total_pages_header": "X-Total"}))
        validate_config(cfg(pagination={"type": "next_link", "next_field": "next"}))

    def test_unknown_pagination_type_raises(self):
        with pytest.raises(ConfigError, match="type inconnu 'zigzag'"):
            validate_config(cfg(pagination={"type": "zigzag"}))

    def test_unknown_pagination_key_raises(self):
        with pytest.raises(ConfigError, match="clé inconnue"):
            validate_config(cfg(pagination={"type": "offset", "limitt": 10}))

    def test_page_key_on_offset_type_raises(self):
        # 'page_param' n'appartient pas à la stratégie offset
        with pytest.raises(ConfigError, match="page_param"):
            validate_config(cfg(pagination={"type": "offset", "page_param": "p"}))

    def test_params_in_body_requires_post(self):
        with pytest.raises(ConfigError, match="POST"):
            validate_config(cfg(pagination={"type": "offset", "params_in": "body"}))

    def test_params_in_body_with_post_is_valid(self):
        validate_config(
            cfg(method="POST", pagination={"type": "offset", "params_in": "body"})
        )

    def test_invalid_params_in_raises(self):
        with pytest.raises(ConfigError, match="params_in"):
            validate_config(cfg(pagination={"type": "offset", "params_in": "header"}))


class TestIncrementalAndRetry:
    def test_valid_incremental(self):
        validate_config(
            cfg(incremental={"enabled": True, "field": "updated_at", "param_name": "since"})
        )

    def test_enabled_without_field_raises(self):
        with pytest.raises(ConfigError, match="'field' requis"):
            validate_config(cfg(incremental={"enabled": True, "param_name": "since"}))

    def test_disabled_incremental_needs_nothing(self):
        validate_config(cfg(incremental={"enabled": False}))

    def test_unknown_incremental_key_raises(self):
        with pytest.raises(ConfigError, match="clé inconnue"):
            validate_config(cfg(incremental={"enabled": True, "field": "f", "param_name": "p", "extra": 1}))

    def test_unknown_retry_key_raises(self):
        with pytest.raises(ConfigError, match="clé inconnue"):
            validate_config(cfg(retry={"max_attempt": 5}))


class TestStaticHeaders:
    def test_valid_headers(self):
        validate_config(cfg(headers={"Prefer": "transient"}))

    def test_headers_must_be_an_object(self):
        with pytest.raises(ConfigError, match="headers"):
            validate_config(cfg(headers="Prefer: transient"))

    def test_secret_reference_in_headers_is_refused(self):
        with pytest.raises(ConfigError, match="auth"):
            validate_config(cfg(headers={"X-Key": {"env_var": "K"}}))


class TestOauth1:
    VALID = {
        "type": "oauth1",
        "realm": "1234567",
        "consumer_key": {"env_var": "CK"},
        "consumer_secret": {"env_var": "CS"},
        "token": {"env_var": "TK"},
        "token_secret": {"env_var": "TS"},
    }

    def test_valid_tba_config(self):
        validate_config(cfg(auth=self.VALID))

    def test_two_legged_without_token_is_valid(self):
        auth = {k: v for k, v in self.VALID.items() if not k.startswith("token")}
        validate_config(cfg(auth=auth))

    def test_token_without_secret_raises(self):
        auth = {k: v for k, v in self.VALID.items() if k != "token_secret"}
        with pytest.raises(ConfigError, match="paire"):
            validate_config(cfg(auth=auth))

    def test_missing_consumer_secret_raises(self):
        auth = {k: v for k, v in self.VALID.items() if k != "consumer_secret"}
        with pytest.raises(ConfigError, match="consumer_secret"):
            validate_config(cfg(auth=auth))

    def test_unknown_signature_method_raises(self):
        with pytest.raises(ConfigError, match="signature_method"):
            validate_config(cfg(auth={**self.VALID, "signature_method": "HMAC-MD5"}))

    def test_sha1_is_accepted(self):
        validate_config(cfg(auth={**self.VALID, "signature_method": "HMAC-SHA1"}))

    def test_unknown_key_raises(self):
        with pytest.raises(ConfigError, match="clé inconnue"):
            validate_config(cfg(auth={**self.VALID, "account_id": "1234567"}))

    def test_env_var_forms_are_accepted(self):
        validate_config(
            cfg(
                auth={
                    "type": "oauth1",
                    "consumer_key_env_var": "CK",
                    "consumer_secret_env_var": "CS",
                    "token_env_var": "TK",
                    "token_secret_env_var": "TS",
                }
            )
        )


class TestIncrementalInject:
    BODY_TEMPLATE = {
        "enabled": True,
        "field": "lastmodified",
        "inject": "body_template",
        "value_format": "iso_datetime",
    }

    def test_valid_body_template(self):
        validate_config(
            cfg(
                method="POST",
                body={"q": "WHERE d >= '{watermark}'"},
                incremental=self.BODY_TEMPLATE,
            )
        )

    def test_body_template_without_body_raises(self):
        with pytest.raises(ConfigError, match="body"):
            validate_config(cfg(incremental=self.BODY_TEMPLATE))

    def test_body_template_does_not_need_param_name(self):
        validate_config(
            cfg(method="POST", body={"q": "{watermark}"}, incremental=self.BODY_TEMPLATE)
        )

    def test_query_param_still_needs_param_name(self):
        with pytest.raises(ConfigError, match="param_name"):
            validate_config(cfg(incremental={"enabled": True, "field": "d"}))

    def test_unknown_inject_raises(self):
        with pytest.raises(ConfigError, match="inject"):
            validate_config(
                cfg(incremental={"enabled": True, "field": "d", "inject": "header"})
            )

    def test_unknown_value_format_raises(self):
        with pytest.raises(ConfigError, match="value_format"):
            validate_config(
                cfg(
                    incremental={
                        "enabled": True,
                        "field": "d",
                        "param_name": "s",
                        "value_format": "epoch",
                    }
                )
            )

    def test_initial_value_is_accepted(self):
        validate_config(
            cfg(
                incremental={
                    "enabled": True,
                    "field": "d",
                    "param_name": "since",
                    "initial_value": "1970-01-01",
                }
            )
        )


class TestRetryAfterConfig:
    def test_max_retry_after_seconds_is_accepted(self):
        validate_config(cfg(retry={"max_attempts": 5, "max_retry_after_seconds": 60}))


class TestBodyTemplateRequiresAnExplicitFormat:
    """Le filtrage des caractères ne protège qu'un placeholder entre quotes.
    Un placeholder nu accepterait `0 OR 1=1` : la forme doit être déclarée."""

    BASE_INC = {"enabled": True, "field": "d", "inject": "body_template"}

    def test_default_any_is_refused(self):
        with pytest.raises(ConfigError, match="value_format"):
            validate_config(
                cfg(method="POST", body={"q": "{watermark}"}, incremental=self.BASE_INC)
            )

    def test_explicit_any_is_refused_too(self):
        with pytest.raises(ConfigError, match="value_format"):
            validate_config(
                cfg(
                    method="POST",
                    body={"q": "{watermark}"},
                    incremental={**self.BASE_INC, "value_format": "any"},
                )
            )

    @pytest.mark.parametrize("fmt", ["numeric", "iso_date", "iso_datetime"])
    def test_declared_formats_are_accepted(self, fmt):
        validate_config(
            cfg(
                method="POST",
                body={"q": "{watermark}"},
                incremental={**self.BASE_INC, "value_format": fmt},
            )
        )

    def test_query_param_mode_still_defaults_to_any(self):
        validate_config(
            cfg(incremental={"enabled": True, "field": "d", "param_name": "since"})
        )
