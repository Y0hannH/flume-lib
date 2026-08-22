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


class TemplateError(Exception):
    pass


def check_value(value, value_format: str = "any", label: str = "valeur") -> str:
    """Valide une valeur avant interpolation. Retourne sa forme texte."""
    text = str(value)
    for token in _FORBIDDEN_TOKENS:
        if token in text:
            raise TemplateError(
                f"{label} : caractère interdit {token!r} dans {text!r} — "
                "une valeur interpolée dans une requête ne peut pas en "
                "modifier la structure"
            )
    pattern = _VALUE_FORMATS.get(value_format)
    if pattern is None and value_format not in _VALUE_FORMATS:
        known = ", ".join(VALUE_FORMATS)
        raise TemplateError(
            f"{label} : 'value_format' inconnu '{value_format}' — attendu l'un de : {known}"
        )
    if pattern is not None and not pattern.match(text):
        raise TemplateError(
            f"{label} : {text!r} ne respecte pas le format '{value_format}'"
        )
    return text


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
                f"placeholder '{{{unknown[0]}}}' sans variable correspondante "
                f"(disponibles : {known})"
            )
        return _PLACEHOLDER_RE.sub(lambda m: str(variables[m.group(1)]), value)
    if isinstance(value, dict):
        return {key: render(item, variables) for key, item in value.items()}
    if isinstance(value, list):
        return [render(item, variables) for item in value]
    return value
