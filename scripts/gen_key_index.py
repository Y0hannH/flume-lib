"""Génère l'index plat des clés de configuration de docs/cookbook.md.

Le cookbook a deux moitiés : une table « mon API fait X → écris Y », écrite à
la main parce qu'elle décrit des symptômes que le code ne connaît pas, et
l'index des clés — celui-ci — qui doit être exhaustif et exact ou ne pas
exister. Un index de référence faux coûte plus cher que pas d'index : on le
consulte au lieu de lire la source, et on y croit.

D'où la génération. Les trois faits que porte chaque ligne viennent chacun du
code, jamais d'une saisie :

  - **la clé et son bloc** : importés de `flume_lib.validation`, qui est
    l'autorité — une clé absente de ces tuples est refusée à la validation ;
  - **le défaut** : lu dans l'implémentation par analyse AST, en cherchant les
    `config.get("clé", défaut)` de auth.py, pagination.py et source.py. C'est
    la seule source honnête : les défauts ne vivent pas dans validation.py,
    ils vivent là où ils sont appliqués ;
  - **le caractère requis** : dérivé de `_REQUIRED` et `_AUTH_REQUIRED` quand
    il y est, et *vérifié empiriquement* sinon (voir `_REQUIRED_INLINE`).

Les exigences inline de `validate_config` — 'cursor_field' avec le type
cursor, 'field' avec incremental.enabled… — sont les seules à ne pas être
introspectables : elles sont écrites en dur dans le corps de la fonction. Elles
sont donc déclarées ici, puis **prouvées** : pour chacune, on construit un
config valide, on retire la clé, et on exige que validate_config proteste. Une
exigence qui disparaît de la lib fait échouer ce script, pas la doc.

Usage :
    python scripts/gen_key_index.py            réécrit le bloc du cookbook
    python scripts/gen_key_index.py --check     sort en 1 si le bloc a divergé

Sans dépendance : stdlib seule, plus la lib elle-même.
"""

import argparse
import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from flume_lib import validation as V  # noqa: E402
from flume_lib.oauth1 import SIGNATURE_METHODS  # noqa: E402
from flume_lib.templating import NORMALIZERS, VALUE_FORMATS  # noqa: E402

COOKBOOK = ROOT / "docs" / "cookbook.md"
BEGIN = "<!-- BEGIN GENERATED KEY INDEX -->"
END = "<!-- END GENERATED KEY INDEX -->"


def _enum(values) -> str:
    """`a`, `b` ou `c` — pour les notes qui listent un domaine de valeurs."""
    quoted = [f"`{v}`" for v in values]
    return " / ".join(quoted)


_ENUM_PARAMS_IN = _enum(V._PARAMS_IN)
_ENUM_VALUE_FORMATS = _enum(VALUE_FORMATS)
_ENUM_NORMALIZERS = _enum(NORMALIZERS)
_ENUM_INJECTS = _enum(V._INCREMENTAL_INJECTS)
_ENUM_WRITE_MODES = _enum(V._WRITE_MODES)
_ENUM_SIGNATURES = _enum(sorted(SIGNATURE_METHODS))



# ---------------------------------------------------------------------------
# 1. Les défauts, lus dans l'implémentation
# ---------------------------------------------------------------------------

# (module, fonction) -> portée. Une fonction absente de cette table est ignorée
# : ses `.get()` ne décrivent pas une clé de config publique.
_SCOPES = {
    ("pagination.py", "paginate_offset"): "pagination.offset",
    ("pagination.py", "paginate_page"): "pagination.page",
    ("pagination.py", "paginate_cursor"): "pagination.cursor",
    ("pagination.py", "paginate_keyset"): "pagination.keyset",
    ("pagination.py", "paginate_next_link"): "pagination.next_link",
    ("pagination.py", "_bounded"): "pagination.*",
    ("auth.py", "_fetch_oauth2_client_credentials"): "auth.oauth2_client_credentials",
    ("auth.py", "_fetch_token_endpoint"): "auth.token_endpoint",
    ("auth.py", "_build_oauth1"): "auth.oauth1",
    # _build_headers traite tous les types dans une suite de `if auth_type ==`
    # : la portée y est affinée branche par branche, ci-dessous.
    ("auth.py", "_build_headers"): "auth.*",
    ("source.py", "_build_wait"): "retry",
    ("source.py", "_check_response_errors"): "errors",
    ("source.py", "_build_fetch_page"): "config",
    ("source.py", "run_source"): "config",
}

# (module, nom du dict porteur) -> bloc, quand le porteur désigne un autre bloc
# que celui de la fonction qui l'entoure. Un porteur mappé ici l'emporte sur la
# portée de la fonction — c'est ce qui rattrape `write.get("mode")`, lu dans la
# méthode d'une classe plutôt que dans une fonction listée ci-dessus.
_RECEIVERS = {
    ("source.py", "retry_config"): "retry",
    ("source.py", "errors_config"): "errors",
    ("source.py", "incremental"): "incremental",
    ("source.py", "write"): "write",
    # Dans source.py, `pagination_config` ne sert qu'aux clés communes : les
    # clés propres à une stratégie sont lues dans pagination.py, où la fonction
    # dit déjà laquelle.
    ("source.py", "pagination_config"): "pagination.*",
}

# Porteurs dont les `.get()` ne décrivent aucune clé publique.
_IGNORED_RECEIVERS = {"payload", "headers", "node", "record", "row"}


class _DefaultCollector(ast.NodeVisitor):
    """Collecte les (portée, clé, défaut) des appels `x.get("clé", défaut)`."""

    def __init__(self, module: str):
        self.module = module
        self.scope = None
        self.branch = None
        self.found: dict[tuple[str, str], str] = {}

    def visit_FunctionDef(self, node):  # noqa: N802
        previous = self.scope
        self.scope = _SCOPES.get((self.module, node.name), None)
        self.generic_visit(node)
        self.scope = previous

    def visit_If(self, node):  # noqa: N802
        # `if auth_type == "bearer_token":` — affine la portée dans la branche
        branch = _auth_type_tested(node.test)
        if branch is not None:
            previous, self.branch = self.branch, f"auth.{branch}"
            for child in node.body:
                self.visit(child)
            self.branch = previous
            for child in node.orelse:
                self.visit(child)
            return
        self.generic_visit(node)

    def visit_Call(self, node):  # noqa: N802
        self.generic_visit(node)
        if not (isinstance(node.func, ast.Attribute) and node.func.attr == "get"):
            return
        if not isinstance(node.func.value, ast.Name):
            return
        if not node.args or not isinstance(node.args[0], ast.Constant):
            return
        key = node.args[0].value
        if not isinstance(key, str):
            return

        receiver = node.func.value.id
        if receiver in _IGNORED_RECEIVERS:
            return
        # Un porteur explicitement mappé nomme son bloc lui-même ; sinon la
        # branche `if auth_type == ...` en cours, sinon la fonction.
        scope = _RECEIVERS.get((self.module, receiver)) or self.branch or self.scope
        if scope in (None, "auth.*"):
            return

        default = _literal(node.args[1]) if len(node.args) > 1 else None
        self.found.setdefault((scope, key), default)


def _auth_type_tested(test) -> str | None:
    if not isinstance(test, ast.Compare) or len(test.ops) != 1:
        return None
    if not isinstance(test.ops[0], ast.Eq):
        return None
    if not (isinstance(test.left, ast.Name) and test.left.id == "auth_type"):
        return None
    right = test.comparators[0]
    return right.value if isinstance(right, ast.Constant) else None


def _literal(node) -> str | None:
    """Forme lisible d'un défaut littéral, ou None quand il n'en est pas un
    (un appel, une constante nommée : la doc dira alors « aucun »)."""
    try:
        value = ast.literal_eval(node)
    except ValueError:
        # DEFAULT_TIMEOUT_SECONDS et consorts : on résout depuis le module
        if isinstance(node, ast.Name) and node.id.isupper():
            return _named_constant(node.id)
        return None
    if value is None:
        return None
    if isinstance(value, str):
        return f'`"{value}"`'
    if isinstance(value, bool):
        return f"`{str(value).lower()}`"
    return f"`{value:,}`".replace(",", " ") if isinstance(value, int) else f"`{value}`"


def _named_constant(name: str) -> str | None:
    from flume_lib import auth, source

    for module in (source, auth):
        if hasattr(module, name):
            value = getattr(module, name)
            if isinstance(value, str):
                return f'`"{value}"`'
            if isinstance(value, int):
                return "`" + f"{value:,}".replace(",", " ") + "`"
            return f"`{value}`"
    return None


def collect_defaults() -> dict[tuple[str, str], str]:
    defaults: dict[tuple[str, str], str] = {}
    for filename in ("pagination.py", "auth.py", "source.py"):
        tree = ast.parse((ROOT / "src" / "flume_lib" / filename).read_text(encoding="utf-8"))
        collector = _DefaultCollector(filename)
        collector.visit(tree)
        for key, value in collector.found.items():
            defaults.setdefault(key, value)
    return defaults


# ---------------------------------------------------------------------------
# 2. Le caractère requis, prouvé
# ---------------------------------------------------------------------------

_BASE = {
    "base_url": "https://api.example.com/v1/items",
    "target_schema": "bronze",
    "target_table": "items",
}

# Exigences écrites en dur dans le corps de validate_config : pas de tuple à
# importer, donc déclarées ici avec le config qui les déclenche. Chacune est
# vérifiée au moment de la génération — retirer la clé du config doit lever.
_REQUIRED_INLINE = (
    ("pagination.cursor", "cursor_param", {
        "pagination": {"type": "cursor", "cursor_param": "c", "cursor_field": "n"}}),
    ("pagination.cursor", "cursor_field", {
        "pagination": {"type": "cursor", "cursor_param": "c", "cursor_field": "n"}}),
    ("pagination.keyset", "key_field", {
        "pagination": {"type": "keyset", "key_field": "id", "key_param": "since"}}),
    ("pagination.keyset", "key_param", {
        "pagination": {"type": "keyset", "key_field": "id", "key_param": "since"}}),
    ("incremental", "field", {
        "incremental": {"enabled": True, "field": "updated_at", "param_name": "since"}}),
    ("incremental", "param_name", {
        "incremental": {"enabled": True, "field": "updated_at", "param_name": "since"}}),
    ("write", "replace_where", {
        "write": {"mode": "replace_where", "replace_where": "id > 0"}}),
)


def prove_required() -> set[tuple[str, str]]:
    """Vérifie chaque exigence inline en la violant. Une exigence qui ne se
    fait plus sentir est une ligne de doc devenue fausse : on refuse de
    générer plutôt que de publier l'erreur."""
    proven = set()
    for scope, key, fragment in _REQUIRED_INLINE:
        block = next(iter(fragment))
        broken = {**_BASE, **{block: {k: v for k, v in fragment[block].items() if k != key}}}
        try:
            V.validate_config(broken)
        except V.ConfigError:
            proven.add((scope, key))
            continue
        raise SystemExit(
            f"gen_key_index : '{key}' est documenté requis dans {scope}, mais "
            f"validate_config accepte un config sans lui. Corriger "
            f"_REQUIRED_INLINE — ou la validation."
        )
    return proven


# ---------------------------------------------------------------------------
# 3. Les lignes
# ---------------------------------------------------------------------------

# Notes attachées à une clé, quand le nom seul induit en erreur. Rédigées en
# anglais comme le reste de docs/, et volontairement courtes : l'index oriente,
# configuration.md explique.
_NOTES = {
    ("config", "name"): "identity in `log_runs` and `watermark`",
    ("config", "params"): "fixed query params, sent on every call beside the pagination params",
    ("config", "headers"): "literal strings only — a credential belongs in `auth`",
    ("config", "body"): 'rejected on GET — set `"method": "POST"`',
    ("config", "template_paths"): "restricts body templating to these branches (GraphQL braces)",
    ("config", "batch_size"): "rows per Delta commit; caps the memory a run uses",
    ("pagination.*", "items_field"):
        "dotted path; without it, `data`/`items`/`results`/`value` are probed",
    ("pagination.*", "record_field"): "extracts a sub-object out of each record",
    ("pagination.*", "params_in"): f"one of {_ENUM_PARAMS_IN}",
    ("pagination.*", "params_path"):
        'nests the params inside the body; requires `"params_in": "body"`',
    ("pagination.*", "max_pages"): "safety bound; the run fails when it is hit",
    ("pagination.*", "max_rows"): "safety bound; the run fails when it is hit",
    ("pagination.offset", "limit"): "also the stop condition — a shorter page ends the loop",
    ("pagination.page", "size_param"):
        "no effect unless `page_size` is set too — the pair goes together",
    ("pagination.page", "page_size"): "no effect unless `size_param` is set too",
    ("pagination.page", "total_pages_header"):
        "read from the first response; absent or non-numeric fails the run",
    ("pagination.cursor", "has_more_field"):
        "set it whenever the API provides it — an empty mid-result page otherwise reads as the end",
    ("pagination.next_link", "next_field"): "top-level key, not a dotted path",
    ("pagination.keyset", "value_format"):
        f"one of {_ENUM_VALUE_FORMATS}; an explicit value is mandatory with `body_template`",
    ("pagination.keyset", "initial_value"): "floor of the first request",
    ("incremental", "field"): "field of the *record*; its max becomes the next watermark",
    ("incremental", "inject"): f"one of {_ENUM_INJECTS}",
    ("incremental", "placeholder"): 'name substituted in `body` with `"inject": "body_template"`',
    ("incremental", "value_format"): f"one of {_ENUM_VALUE_FORMATS}",
    ("incremental", "normalize"):
        f"one of {_ENUM_NORMALIZERS}; reshapes the watermark before it is "
        "**sent**, not as stored",
    ("incremental", "checkpoint"):
        "commits the watermark batch by batch; incompatible with any `write.mode` but `append`",
    ("write", "mode"): f"one of {_ENUM_WRITE_MODES}",
    ("write", "replace_where"): "SQL predicate over the target table; not templated",
    ("write", "partition_by"): "Delta partition columns",
    ("retry", "max_retry_after_seconds"): "ceiling on a `Retry-After` the server asks for",
    ("errors", "path"): "where the error envelope sits in a 200 response",
    ("errors", "retryable_codes"): "codes retried like a 5xx instead of failing the run",
    ("auth.bearer_token", "value_prefix"):
        'raw concatenation, trailing space included; `""` sends the bare token',
    ("auth.oauth1", "token"): "goes with `token_secret`; omitting both gives the two-legged flavor",
    ("auth.oauth1", "token_secret"): "goes with `token`",
    ("auth.oauth1", "signature_method"): f"one of {_ENUM_SIGNATURES}",
    ("auth.oauth1", "realm"): "sent in the header, outside the signature",
    ("auth.oauth2_client_credentials", "tenant_id"):
        "Entra ID shortcut; builds the token URL instead of `token_url`",
    ("auth.token_endpoint", "expires_in_json_path"):
        "without it, the token is only renewed after a 401",
    ("auth.token_endpoint", "timeout_seconds"):
        "of the login call only, independent of the data calls",
    ("auth.oauth2_client_credentials", "timeout_seconds"): "of the token call only",
}

# Clés dont la valeur est un bloc : le `{}` que lit l'implémentation est un
# détail d'appel, pas un défaut que le lecteur doive connaître.
_BLOCK_KEYS = {
    ("config", "auth"), ("config", "pagination"), ("config", "incremental"),
    ("config", "retry"), ("config", "errors"), ("config", "write"),
}


def _label(scope: str) -> str:
    """Titre de section pour une portée."""
    if scope == "config":
        return "Top level"
    if scope == "pagination.*":
        return "`pagination` — common to every strategy"
    if scope.startswith("pagination."):
        return f'`pagination` — `"type": "{scope.split(".", 1)[1]}"`'
    if scope.startswith("auth."):
        return f'`auth` — `"type": "{scope.split(".", 1)[1]}"`'
    return f"`{scope}`"


def _scopes() -> list[tuple[str, tuple[str, ...], set[str]]]:
    """(portée, clés, clés requises) dans l'ordre de la page."""
    out: list[tuple[str, tuple[str, ...], set[str]]] = [
        ("config", V._REQUIRED + V._OPTIONAL, set(V._REQUIRED)),
        ("pagination.*", V._PAGINATION_COMMON, set()),
    ]
    for name in ("offset", "page", "cursor", "keyset", "next_link"):
        out.append((f"pagination.{name}", V._PAGINATION_KEYS[name], set()))
    for name, keys in V._AUTH_KEYS.items():
        if not keys:
            continue
        required = {k for group in V._AUTH_REQUIRED.get(name, ()) for k in group}
        out.append((f"auth.{name}", keys, required))
    out.append(("incremental", V._INCREMENTAL_KEYS, set()))
    out.append(("write", V._WRITE_KEYS, set()))
    out.append(("retry", V._RETRY_KEYS, set()))
    out.append(("errors", V._ERRORS_KEYS, set()))
    return out


def render() -> str:
    defaults = collect_defaults()
    proven = prove_required()

    lines = [
        BEGIN,
        "",
        "<!-- Généré par scripts/gen_key_index.py — ne pas éditer à la main. -->",
        "",
    ]
    for scope, keys, required in _scopes():
        inline = {key for (s, key) in proven if s == scope}
        lines.append(f"#### {_label(scope)}")
        lines.append("")
        lines.append("| Key | Required | Default | Note |")
        lines.append("|---|---|---|---|")
        for key in keys:
            if key in required:
                # 'token' ou 'token_env_var' : requis en groupe, pas seul
                groups = V._AUTH_REQUIRED.get(scope.split(".", 1)[-1], ())
                group = next((g for g in groups if key in g), ())
                mark = "one of" if len(group) > 1 else "yes"
            elif key in inline:
                mark = "conditional"
            else:
                mark = "—"
            default = "—" if (scope, key) in _BLOCK_KEYS else (defaults.get((scope, key)) or "—")
            note = _NOTES.get((scope, key), "")
            lines.append(f"| `{key}` | {mark} | {default} | {note} |")
        lines.append("")
    lines.append(END)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="ne rien écrire ; sortir en 1 si le bloc a divergé")
    args = parser.parse_args()

    generated = render()
    text = COOKBOOK.read_text(encoding="utf-8")
    if BEGIN not in text or END not in text:
        print(f"gen_key_index : marqueurs {BEGIN} / {END} absents de {COOKBOOK.name}")
        return 1

    head, _, rest = text.partition(BEGIN)
    _, _, tail = rest.partition(END)
    updated = f"{head}{generated}{tail}"

    if updated == text:
        print("gen_key_index : l'index est à jour")
        return 0
    if args.check:
        print(
            "gen_key_index : docs/cookbook.md a divergé du code.\n"
            "  Regénérer : python scripts/gen_key_index.py"
        )
        return 1
    COOKBOOK.write_text(updated, encoding="utf-8")
    print(f"gen_key_index : {COOKBOOK.relative_to(ROOT)} regénéré")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
