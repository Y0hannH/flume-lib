"""Substitution de variables dans les valeurs de config (`body`, `params`).

Nécessaire pour les APIs dont le filtre incrémental vit dans le corps de la
requête plutôt qu'en query string — typiquement une API SQL-over-REST où le
watermark doit atterrir dans la clause WHERE, hors de portée de
`incremental.param_name`.

Volontairement pas `str.format()` : une chaîne SQL ou JSON contenant une
accolade la ferait échouer, et `format()` expose l'accès aux attributs des
valeurs passées. Ici seuls les placeholders `{nom}` connus sont remplacés, et
un placeholder sans variable correspondante est une erreur — même logique que
`validate_config` : une faute de frappe ne doit jamais partir en silence.
"""

import re
from datetime import datetime, timezone

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Une valeur interpolée dans une requête (SQL, filtre OData…) ne doit pas
# pouvoir en changer la structure. Un watermark légitime est un identifiant
# ou une date : aucun de ces caractères n'y figure. Contrôle non désactivable
# — `value_format` ne fait que restreindre davantage.
_FORBIDDEN_TOKENS = ("'", '"', ";", "--", "\\", "\n", "\r", "\x00", "/*")

_VALUE_FORMATS = {
    "any": None,
    "numeric": re.compile(r"^-?\d+(\.\d+)?$"),
    "iso_date": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    "iso_datetime": re.compile(
        r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
    ),
}

VALUE_FORMATS = tuple(_VALUE_FORMATS)

# Normalisations applicables au watermark avant son injection dans la requête.
# Une API qui renvoie ses dates dans un fuseau local (« 2026-08-25T14:57:44.000
# +02:00 ») mais n'accepte que de l'UTC dans son filtre rendait l'incrémental
# inexploitable : la lib réinjecte la valeur exactement telle qu'elle l'a lue,
# et rien ne permettait de la reformer entre les deux.
NORMALIZERS = ("none", "utc_iso")

# Formes ISO 8601 acceptées en entrée de la normalisation. Volontairement plus
# permissif que `_VALUE_FORMATS["iso_datetime"]` sur l'offset (« +0200 » sans
# deux-points) et sur le nombre de décimales : ce qui entre ici vient de l'API,
# pas de la config, et se subit plutôt qu'il ne se choisit.
_ISO_INPUT_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})[ T]"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<offset>[Zz]|[+-]\d{2}:?\d{2})?$"
)


class TemplateError(Exception):
    pass


def check_value(value, value_format: str = "any", label: str = "value") -> str:
    """Valide une valeur avant interpolation. Retourne sa forme texte."""
    text = str(value)
    for token in _FORBIDDEN_TOKENS:
        if token in text:
            raise TemplateError(
                f"{label}: forbidden character {token!r} in {text!r} — "
                "a value interpolated into a request cannot alter its "
                "structure"
            )
    pattern = _VALUE_FORMATS.get(value_format)
    if pattern is None and value_format not in _VALUE_FORMATS:
        known = ", ".join(VALUE_FORMATS)
        raise TemplateError(
            f"{label}: unknown 'value_format' '{value_format}' — "
            f"expected one of: {known}"
        )
    if pattern is not None and not pattern.match(text):
        raise TemplateError(
            f"{label}: {text!r} does not match format '{value_format}'"
        )
    return text


def _to_utc_iso(text: str, label: str) -> str:
    """Ramène un instant ISO 8601 à l'UTC, en millisecondes suffixées `Z`.

    Une valeur sans fuseau est lue comme de l'UTC. C'est la convention de
    `pandas.to_datetime(..., utc=True)`, et surtout le seul choix stable : la
    rattacher au fuseau de la machine ferait dépendre la borne d'un run de
    l'endroit où tourne le notebook.
    """
    match = _ISO_INPUT_RE.match(text)
    if not match:
        raise TemplateError(
            f"{label}: {text!r} is not an ISO 8601 instant — "
            "'normalize' cannot convert it to UTC"
        )
    # fromisoformat n'accepte que 3 ou 6 décimales avant Python 3.11, et pas
    # le suffixe 'Z' : les deux sont ramenés ici à une forme qu'il connaît.
    frac = (match.group("frac") or "").ljust(6, "0")[:6]
    offset = match.group("offset") or ""
    if offset in ("Z", "z") or not offset:
        offset = "+00:00"
    elif ":" not in offset:
        offset = f"{offset[:3]}:{offset[3:]}"
    stamp = f"{match.group('date')}T{match.group('time')}.{frac}{offset}"
    moment = datetime.fromisoformat(stamp).astimezone(timezone.utc)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"


def normalize_value(value, normalize: str = "none", label: str = "value"):
    """Reforme la valeur du watermark avant qu'elle reparte vers l'API.

    `none` la laisse intacte — c'est le défaut, et le comportement historique.
    `utc_iso` la ramène à l'UTC : une API qui date ses enregistrements dans un
    fuseau local mais ne filtre qu'en UTC devient utilisable sans code
    d'adaptation dans le notebook.

    La normalisation ne touche que ce qui est **envoyé**. Le watermark reste
    stocké tel que l'API l'a écrit, et le `max()` d'un lot continue de porter
    sur les valeurs brutes des enregistrements.
    """
    if normalize not in NORMALIZERS:
        known = ", ".join(NORMALIZERS)
        raise TemplateError(
            f"{label}: unknown 'normalize' '{normalize}' — "
            f"expected one of: {known}"
        )
    if normalize == "none":
        return value
    return _to_utc_iso(str(value).strip(), label)


def placeholders(value) -> set[str]:
    """Noms des `{placeholders}` présents dans les chaînes de `value`.

    Sert à répartir les paramètres de pagination entre le corps et la query
    string : un paramètre dont le placeholder figure dans le corps y est
    substitué, les autres partent en query string. Aucun paramètre ne peut
    donc être perdu en route.
    """
    if isinstance(value, str):
        return set(_PLACEHOLDER_RE.findall(value))
    if isinstance(value, dict):
        return set().union(*(placeholders(v) for v in value.values()), set())
    if isinstance(value, list):
        return set().union(*(placeholders(v) for v in value), set())
    return set()


def templated_placeholders(value, template_paths=None) -> set[str]:
    """Placeholders que `value` sait accueillir, restreints aux branches de
    `template_paths` quand elles sont déclarées — le même périmètre que le
    rendu. Utilisé au runtime pour répartir les paramètres entre corps et query
    string, et à la validation pour vérifier que la clé de pagination a bien
    une place où atterrir. Une seule implémentation : deux versions qui
    divergeraient laisseraient passer une config que le runtime saboterait."""
    if not template_paths:
        return placeholders(value)
    names: set[str] = set()
    for path in template_paths:
        node = value
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                node = None
                break
            node = node[part]
        names |= placeholders(node)
    return names


def render(value, variables: dict):
    """Remplace récursivement les `{nom}` dans les chaînes de `value`.
    Sans variables, la valeur est renvoyée telle quelle — aucun contrôle de
    placeholder n'est appliqué, pour ne pas casser les configs qui n'utilisent
    pas le templating."""
    if not variables:
        return value
    if isinstance(value, str):
        unknown = [
            name for name in _PLACEHOLDER_RE.findall(value) if name not in variables
        ]
        if unknown:
            known = ", ".join(sorted(variables))
            raise TemplateError(
                f"placeholder '{{{unknown[0]}}}' has no matching variable "
                f"(available: {known})"
            )
        return _PLACEHOLDER_RE.sub(lambda m: str(variables[m.group(1)]), value)
    if isinstance(value, dict):
        return {key: render(item, variables) for key, item in value.items()}
    if isinstance(value, list):
        return [render(item, variables) for item in value]
    return value
