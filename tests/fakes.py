"""Doublures HTTP partagées par la suite.

`FakeResponse` est une vraie `requests.Response`, au corps déjà chargé : le
code de production la lit par `iter_content`, exactement comme une réponse du
réseau. Une doublure qui n'offrait que `.json()` obligeait la lib à garder une
branche pour elle — et la lecture en flux, avec ses plafonds, n'était alors
exercée que par une poignée de tests.
"""

import json

import requests
from requests.structures import CaseInsensitiveDict


class FakeResponse(requests.Response):
    """Réponse HTTP dont le corps est `payload` sérialisé en JSON, ou `text`
    tel quel (un corps d'erreur, du HTML, du JSON invalide)."""

    def __init__(self, payload=None, headers=None, status_code=200, text=None):
        super().__init__()
        body = json.dumps(payload) if text is None else text
        self._content = body.encode("utf-8")
        self._content_consumed = True
        self.status_code = status_code
        self.headers = CaseInsensitiveDict(headers or {})
        self.encoding = "utf-8"
        self.url = "https://fake.test/"


class SinglePageSession:
    """Session qui sert la même page à chaque requête : `payload` est la liste
    d'enregistrements, enveloppée sous `items_field` s'il est donné. Pour les
    tests qui portent sur l'écriture, pas sur la pagination."""

    payload: list = []
    items_field: str | None = None

    def __init__(self):
        self.headers = {}
        self.closed = False

    def close(self):
        self.closed = True

    def request(self, method, url, **kwargs):
        cls = type(self)
        body = {cls.items_field: cls.payload} if cls.items_field else cls.payload
        return FakeResponse(body)
