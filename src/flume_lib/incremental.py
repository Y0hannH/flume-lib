"""Logique pure du watermark : comparer deux bornes, les ramener à une date,
les décaler. Aucune entrée-sortie ici — `source.py` s'en sert pour décider ce
qui s'écrit, `windows.py` pour découper une période, `validation.py` pour
refuser une fenêtre incohérente avant le premier appel.

Le watermark est relu de la table Delta sous forme de texte, alors que le
`max()` d'un lot porte sur les valeurs brutes des enregistrements : un champ
entier donne `150` d'un côté et `"100"` de l'autre. Les comparer tel quel lève
`TypeError`, ce que l'ancien code lisait comme « pas une régression » — et
laissait donc passer. `align` ramène les deux côtés à un type commun avant de
trancher."""

import re
from datetime import date

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_date(value) -> date | None:
    """La date d'une valeur `yyyy-MM-dd`, ou None si ce n'en est pas une.
    Une date de calendrier invalide (`2026-13-45`) n'en est pas une non plus."""
    if not isinstance(value, str) or not _DATE_ONLY_RE.match(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _is_number(value) -> bool:
    # bool est un int en Python : True ne doit pas passer pour 1
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _parse_number(text: str):
    for cast in (int, float):
        try:
            return cast(text.strip())
        except ValueError:
            continue
    return None


def align(left, right):
    """`(left, right)` ramenés à un type commun comparable, ou None quand ils
    ne le sont pas.

    Deux dates `yyyy-MM-dd` sont comparées comme des dates ; deux textes
    quelconques lexicographiquement (la convention de la lib pour les
    horodatages ISO 8601) ; un nombre et un texte numérique comme des nombres.
    Tout le reste est incomparable."""
    if isinstance(left, str) and isinstance(right, str):
        left_date, right_date = parse_date(left), parse_date(right)
        if left_date is not None and right_date is not None:
            return left_date, right_date
        return left, right
    if _is_number(left) and _is_number(right):
        return left, right
    if _is_number(left) and isinstance(right, str):
        converted = _parse_number(right)
        return None if converted is None else (left, converted)
    if isinstance(left, str) and _is_number(right):
        converted = _parse_number(left)
        return None if converted is None else (converted, right)
    return None


def compare(left, right) -> int | None:
    """-1, 0 ou 1 ; None si les deux valeurs sont incomparables."""
    pair = align(left, right)
    if pair is None:
        return None
    first, second = pair
    return (first > second) - (first < second)
