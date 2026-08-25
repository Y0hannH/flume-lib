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
        with pytest.raises(ConfigError, match="unknown key 'foo'"):
            validate_config(cfg(foo=1))

    def test_typo_suggests_correct_key(self):
        # le bug silencieux d'origine : 'pagintaion' ignoré = appel unique
        with pytest.raises(ConfigError, match="did you mean 'pagination'"):
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
        with pytest.raises(ConfigError, match="unknown type 'jwt'"):
            validate_config(cfg(auth={"type": "jwt"}))

    def test_unknown_auth_key_raises(self):
        with pytest.raises(ConfigError, match="unknown key 'tokenn'"):
            validate_config(cfg(auth={"type": "bearer_token", "tokenn": "x"}))

    def test_missing_credential_raises(self):
        with pytest.raises(ConfigError, match="'token'"):
            validate_config(cfg(auth={"type": "bearer_token", "header_name": "X"}))

    def test_oauth2_without_tenant_or_url_raises(self):
        with pytest.raises(ConfigError, match="token_url"):
            validate_config(
                cfg(auth={
                    "type": "oauth2_client_credentials",
                    "client_id": "c",
                    "client_secret": "s",
                })
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
        with pytest.raises(ConfigError, match="unknown type 'zigzag'"):
            validate_config(cfg(pagination={"type": "zigzag"}))

    def test_unknown_pagination_key_raises(self):
        with pytest.raises(ConfigError, match="unknown key"):
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
        with pytest.raises(ConfigError, match="'field' required"):
            validate_config(cfg(incremental={"enabled": True, "param_name": "since"}))

    def test_valid_normalize(self):
        validate_config(
            cfg(incremental={
                "enabled": True, "field": "updated_at", "param_name": "since",
                "normalize": "utc_iso",
            })
        )

    def test_unknown_normalize_raises(self):
        with pytest.raises(ConfigError, match="unknown 'normalize'"):
            validate_config(
                cfg(incremental={
                    "enabled": True, "field": "updated_at", "param_name": "since",
                    "normalize": "utc",
                })
            )

    def test_disabled_incremental_needs_nothing(self):
        validate_config(cfg(incremental={"enabled": False}))

    def test_unknown_incremental_key_raises(self):
        with pytest.raises(ConfigError, match="unknown key"):
            validate_config(cfg(incremental={
                "enabled": True, "field": "f", "param_name": "p", "extra": 1,
            }))

    def test_unknown_retry_key_raises(self):
        with pytest.raises(ConfigError, match="unknown key"):
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
        with pytest.raises(ConfigError, match="go together"):
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
        with pytest.raises(ConfigError, match="unknown key"):
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


class TestErrorsSection:
    def test_valid_graphql_envelope(self):
        validate_config(cfg(errors={"path": "errors", "retryable_codes": ["THROTTLED"]}))

    def test_defaults_only_is_valid(self):
        validate_config(cfg(errors={}))

    def test_unknown_key_raises(self):
        with pytest.raises(ConfigError, match="retryable_codes"):
            validate_config(cfg(errors={"retryable_code": ["X"]}))

    def test_non_object_raises(self):
        with pytest.raises(ConfigError, match="errors"):
            validate_config(cfg(errors="errors"))

    def test_empty_path_raises(self):
        with pytest.raises(ConfigError, match="path"):
            validate_config(cfg(errors={"path": ""}))

    def test_non_list_retryable_codes_raises(self):
        with pytest.raises(ConfigError, match="retryable_codes"):
            validate_config(cfg(errors={"retryable_codes": "THROTTLED"}))


class TestTemplatePaths:
    BODY = {"query": "{orders{id}}", "variables": {"q": "updated_at:>'{watermark}'"}}

    def test_valid_path(self):
        validate_config(cfg(method="POST", body=self.BODY, template_paths=["variables"]))

    def test_nested_path(self):
        body = {"outer": {"inner": {"q": "{watermark}"}}}
        validate_config(cfg(method="POST", body=body, template_paths=["outer.inner"]))

    def test_unknown_path_raises(self):
        with pytest.raises(ConfigError, match="variabels"):
            validate_config(
                cfg(method="POST", body=self.BODY, template_paths=["variabels"])
            )

    def test_path_through_a_non_object_raises(self):
        with pytest.raises(ConfigError, match="query"):
            validate_config(
                cfg(method="POST", body=self.BODY, template_paths=["query.inner"])
            )

    def test_without_body_raises(self):
        with pytest.raises(ConfigError, match="body"):
            validate_config(cfg(template_paths=["variables"]))

    def test_non_list_raises(self):
        with pytest.raises(ConfigError, match="template_paths"):
            validate_config(cfg(method="POST", body=self.BODY, template_paths="variables"))


class TestCursorPaginationConfig:
    GRAPHQL_BODY = {"query": "{orders{id}}", "variables": {}}

    def cursor_cfg(self, **overrides):
        pagination = {
            "type": "cursor",
            "cursor_param": "after",
            "cursor_field": "data.orders.pageInfo.endCursor",
            "has_more_field": "data.orders.pageInfo.hasNextPage",
            "items_field": "data.orders.edges",
            "record_field": "node",
            "limit": 250,
            "limit_param": "first",
            "params_in": "body",
            "params_path": "variables",
            **overrides,
        }
        return cfg(method="POST", body=self.GRAPHQL_BODY, pagination=pagination)

    def test_full_connection_config_is_valid(self):
        validate_config(self.cursor_cfg())

    @pytest.mark.parametrize("key", ["cursor_param", "cursor_field"])
    def test_missing_required_cursor_key_raises(self, key):
        config = self.cursor_cfg()
        del config["pagination"][key]
        with pytest.raises(ConfigError, match=key):
            validate_config(config)

    def test_unknown_cursor_key_raises(self):
        with pytest.raises(ConfigError, match="cursor_field"):
            validate_config(self.cursor_cfg(cursor_fields="x"))

    def test_params_path_requires_params_in_body(self):
        with pytest.raises(ConfigError, match="params_in"):
            validate_config(self.cursor_cfg(params_in="query"))

    def test_params_path_through_a_non_object_raises(self):
        config = self.cursor_cfg(params_path="query")
        with pytest.raises(ConfigError, match="params_path"):
            validate_config(config)

    def test_params_path_may_point_at_an_absent_branch(self):
        """La branche est créée à l'envoi si le corps ne la porte pas encore."""
        config = self.cursor_cfg()
        config["body"] = {"query": "{orders{id}}"}
        validate_config(config)

    def test_record_field_is_accepted_by_every_strategy(self):
        validate_config(
            cfg(pagination={"type": "offset", "record_field": "node"})
        )


class TestBatchSize:
    def test_default_is_implicit(self):
        validate_config(cfg())

    def test_positive_integer_is_valid(self):
        validate_config(cfg(batch_size=1))
        validate_config(cfg(batch_size=100_000))

    @pytest.mark.parametrize("value", [0, -1])
    def test_non_positive_raises(self, value):
        with pytest.raises(ConfigError, match="greater than 0"):
            validate_config(cfg(batch_size=value))

    @pytest.mark.parametrize("value", ["1000", 1000.0, None, True])
    def test_non_integer_raises(self, value):
        with pytest.raises(ConfigError, match="must be an integer"):
            validate_config(cfg(batch_size=value))


class TestIncrementalCheckpoint:
    def test_checkpoint_with_incremental_is_valid(self):
        validate_config(
            cfg(incremental={
                "enabled": True, "field": "ts", "param_name": "since",
                "checkpoint": True,
            })
        )

    def test_checkpoint_without_enabled_raises(self):
        with pytest.raises(ConfigError, match="checkpoint"):
            validate_config(cfg(incremental={"checkpoint": True}))


class TestKeysetPagination:
    def keyset(self, **overrides):
        return {
            "type": "keyset", "key_field": "id", "key_param": "since_id",
            **overrides,
        }

    def test_minimal_keyset_is_valid(self):
        validate_config(cfg(pagination=self.keyset()))

    @pytest.mark.parametrize("key", ["key_field", "key_param"])
    def test_missing_required_key_raises(self, key):
        pagination = self.keyset()
        del pagination[key]
        with pytest.raises(ConfigError, match=key):
            validate_config(cfg(pagination=pagination))

    def test_unknown_keyset_key_raises(self):
        with pytest.raises(ConfigError, match="unknown key"):
            validate_config(cfg(pagination=self.keyset(cursor_param="c")))

    def test_unknown_value_format_raises(self):
        with pytest.raises(ConfigError, match="value_format"):
            validate_config(cfg(pagination=self.keyset(value_format="iso_week")))

    def test_body_template_requires_an_explicit_value_format(self):
        with pytest.raises(ConfigError, match="value_format"):
            validate_config(cfg(
                method="POST",
                body={"q": "select * from t where id > {since_id}"},
                pagination=self.keyset(params_in="body_template"),
            ))

    def test_body_template_with_a_value_format_is_valid(self):
        validate_config(cfg(
            method="POST",
            body={"q": "select * from t where id > {since_id}"},
            pagination=self.keyset(
                params_in="body_template", value_format="numeric"
            ),
        ))


class TestParamsInBodyTemplate:
    BASE_KEYSET = {
        "type": "keyset", "key_field": "id", "key_param": "since_id",
        "params_in": "body_template", "value_format": "numeric",
    }

    def test_requires_a_post(self):
        with pytest.raises(ConfigError, match="POST"):
            validate_config(cfg(pagination=self.BASE_KEYSET))

    def test_requires_a_body(self):
        with pytest.raises(ConfigError, match="body"):
            validate_config(cfg(method="POST", pagination=self.BASE_KEYSET))

    def test_top_level_params_are_allowed(self):
        """Ils partent en query string : seule la clé va dans le corps."""
        validate_config(cfg(
            method="POST",
            body={"q": "{since_id}"},
            params={"status": "active"},
            pagination=self.BASE_KEYSET,
        ))

    def test_a_watermark_as_a_query_param_is_allowed(self):
        """La clé dans le SQL, le watermark en query string : les deux canaux
        sont disponibles."""
        validate_config(cfg(
            method="POST",
            body={"q": "{since_id}"},
            incremental={
                "enabled": True, "field": "ts", "param_name": "since",
            },
            pagination=self.BASE_KEYSET,
        ))

    def test_a_missing_key_placeholder_raises(self):
        with pytest.raises(ConfigError, match="placeholder"):
            validate_config(cfg(
                method="POST",
                body={"q": "select id from t order by id"},
                pagination=self.BASE_KEYSET,
            ))

    def test_a_typo_in_the_key_placeholder_raises(self):
        with pytest.raises(ConfigError, match="sinceid"):
            validate_config(cfg(
                method="POST",
                body={"q": "select id from t where id > {sinceid}"},
                pagination=self.BASE_KEYSET,
            ))

    def test_the_placeholder_must_sit_in_a_declared_template_path(self):
        """Hors des branches de template_paths, il ne serait jamais substitué."""
        with pytest.raises(ConfigError, match="placeholder"):
            validate_config(cfg(
                method="POST",
                body={"q": "id > {since_id}", "vars": {"x": 1}},
                template_paths=["vars"],
                pagination=self.BASE_KEYSET,
            ))

    def test_a_watermark_in_the_body_is_accepted(self):
        validate_config(cfg(
            method="POST",
            body={"q": "where ts > '{watermark}' and id > {since_id}"},
            incremental={
                "enabled": True, "field": "ts", "inject": "body_template",
                "value_format": "iso_datetime", "initial_value": "2020-01-01T00:00:00Z",
            },
            pagination=self.BASE_KEYSET,
        ))

    def test_an_unknown_params_in_raises(self):
        with pytest.raises(ConfigError, match="params_in"):
            validate_config(cfg(pagination={"type": "offset", "params_in": "url"}))


class TestPaginationBounds:
    @pytest.mark.parametrize("key", ["max_pages", "max_rows"])
    def test_positive_integers_are_valid(self, key):
        validate_config(cfg(pagination={"type": "offset", key: 10}))

    @pytest.mark.parametrize("key", ["max_pages", "max_rows"])
    @pytest.mark.parametrize("value", [0, -5])
    def test_non_positive_raises(self, key, value):
        with pytest.raises(ConfigError, match="greater than 0"):
            validate_config(cfg(pagination={"type": "offset", key: value}))

    @pytest.mark.parametrize("key", ["max_pages", "max_rows"])
    @pytest.mark.parametrize("value", ["10", 1.5, True])
    def test_non_integer_raises(self, key, value):
        with pytest.raises(ConfigError, match="must be an integer"):
            validate_config(cfg(pagination={"type": "offset", key: value}))

    def test_bounds_are_accepted_by_every_strategy(self):
        for pagination_type in ("offset", "page", "next_link"):
            validate_config(cfg(pagination={
                "type": pagination_type, "max_pages": 5, "max_rows": 500,
            }))


class TestWrite:
    def test_absent_is_valid(self):
        validate_config(cfg())

    def test_append_is_the_default(self):
        validate_config(cfg(write={}))

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_modes_without_a_predicate(self, mode):
        validate_config(cfg(write={"mode": mode}))

    def test_replace_where_is_valid(self):
        validate_config(cfg(write={
            "mode": "replace_where",
            "replace_where": "trandate >= '2026-01-01' AND trandate < '2026-02-01'",
        }))

    def test_unknown_mode_lists_the_known_ones(self):
        with pytest.raises(ConfigError, match="replace_where"):
            validate_config(cfg(write={"mode": "upsert"}))

    def test_unknown_key_raises(self):
        with pytest.raises(ConfigError, match="unknown key 'replaceWhere'"):
            validate_config(cfg(write={"replaceWhere": "x = 1"}))

    def test_replace_where_mode_without_predicate_raises(self):
        # sans prédicat le remplacement porterait sur la table entière : ça se
        # demande explicitement, ça ne s'obtient pas par omission
        with pytest.raises(ConfigError, match="'replace_where' required"):
            validate_config(cfg(write={"mode": "replace_where"}))

    @pytest.mark.parametrize("mode", ["append", "overwrite"])
    def test_predicate_without_its_mode_raises(self, mode):
        # le piège : le prédicat serait ignoré et l'append (ou l'écrasement
        # total) aurait lieu sans que rien ne le dise
        with pytest.raises(ConfigError, match="is ignored"):
            validate_config(cfg(write={"mode": mode, "replace_where": "m = '01'"}))

    @pytest.mark.parametrize("value", ["", "   ", 42, None])
    def test_an_empty_predicate_raises(self, value):
        with pytest.raises(ConfigError, match="replace_where"):
            validate_config(cfg(write={"mode": "replace_where", "replace_where": value}))

    def test_a_placeholder_in_the_predicate_raises(self):
        # 'replace_where' n'est pas templaté : un {mois} y resterait littéral,
        # ne désignerait aucune ligne, et le run remplacerait le vide
        with pytest.raises(ConfigError, match="is not templated"):
            validate_config(cfg(write={
                "mode": "replace_where", "replace_where": "mois = '{mois}'",
            }))

    def test_partition_by_is_valid(self):
        validate_config(cfg(write={"partition_by": ["year", "month"]}))

    @pytest.mark.parametrize("value", ["year", [], ["year", ""], [1]])
    def test_a_malformed_partition_by_raises(self, value):
        with pytest.raises(ConfigError, match="partition_by"):
            validate_config(cfg(write={"partition_by": value}))

    @pytest.mark.parametrize("mode", ["overwrite", "replace_where"])
    def test_checkpoint_with_a_replacing_mode_raises(self, mode):
        # reprendre au milieu d'un backfill re-remplacerait la fenêtre et
        # effacerait ce que le run interrompu y avait déjà écrit
        write = {"mode": mode}
        if mode == "replace_where":
            write["replace_where"] = "m = '01'"
        with pytest.raises(ConfigError, match="checkpoint"):
            validate_config(cfg(
                write=write,
                incremental={
                    "enabled": True, "field": "ts", "param_name": "since",
                    "checkpoint": True,
                },
            ))

    def test_checkpoint_stays_valid_in_append(self):
        validate_config(cfg(
            write={"mode": "append"},
            incremental={
                "enabled": True, "field": "ts", "param_name": "since",
                "checkpoint": True,
            },
        ))

    def test_a_replacing_mode_without_checkpoint_is_valid(self):
        validate_config(cfg(
            write={"mode": "replace_where", "replace_where": "m = '01'"},
            incremental={"enabled": True, "field": "ts", "param_name": "since"},
        ))


class TestBodyOnGet:
    """Un GET porteur d'un corps est refuse par defaut — la plupart des APIs
    l'ignorent, et le silence coute cher. Certaines l'exigent : l'exception se
    declare."""

    def test_body_on_get_is_refused_by_default(self):
        with pytest.raises(ConfigError, match="ignored on GET"):
            validate_config(cfg(body={"updated_from": "2026-01-01"}))

    def test_body_on_get_is_allowed_once_declared(self):
        validate_config(
            cfg(body={"updated_from": "2026-01-01"}, allow_body_on_get=True)
        )

    def test_body_on_post_needs_no_flag(self):
        validate_config(cfg(method="POST", body={"updated_from": "2026-01-01"}))

    def test_params_in_body_on_get_is_refused_by_default(self):
        # sans cle 'body' : sinon la regle precedente tire la premiere
        with pytest.raises(ConfigError, match="requires a POST method"):
            validate_config(
                cfg(pagination={"type": "offset", "params_in": "body"})
            )

    def test_params_in_body_on_get_is_allowed_once_declared(self):
        validate_config(
            cfg(
                pagination={"type": "offset", "params_in": "body"},
                allow_body_on_get=True,
            )
        )

    @pytest.mark.parametrize("value", ["true", 1, None])
    def test_non_boolean_raises(self, value):
        with pytest.raises(ConfigError, match="must be a boolean"):
            validate_config(cfg(allow_body_on_get=value))

