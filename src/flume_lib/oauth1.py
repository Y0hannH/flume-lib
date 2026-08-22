"""Signature OAuth 1.0a (RFC 5849) — authentification calculée par requête.

Contrairement aux autres stratégies d'auth, la signature couvre la méthode,
l'URL et les paramètres de *chaque* requête : elle ne peut pas être calculée
une fois pour toute la session. L'objet renvoyé est un
`requests.auth.AuthBase`, posé sur `session.auth` et invoqué par requests au
moment de préparer chaque appel — donc après que la pagination a fixé l'URL
finale.

Implémentation stdlib volontaire : aucune dépendance supplémentaire à figer
dans le lot de wheels Fabric.

Standard générique (NetSuite TBA, Xero historique, WooCommerce…), pas un
connecteur dédié à un fournisseur.
"""

import base64
import hashlib
import hmac
import secrets
import time
import urllib.parse

from requests.auth import AuthBase

# NetSuite impose HMAC-SHA256 ; HMAC-SHA1 reste accepté pour les APIs plus
# anciennes qui ne connaissent que lui.
SIGNATURE_METHODS = {
    "HMAC-SHA256": hashlib.sha256,
    "HMAC-SHA1": hashlib.sha1,
}

FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"
_DEFAULT_PORTS = {"http": 80, "https": 443}


def _quote(value) -> str:
    """Percent-encoding RFC 3986 : seuls A-Z a-z 0-9 - . _ ~ restent bruts.
    `safe=""` est indispensable — le défaut de urllib laisserait passer '/'."""
    return urllib.parse.quote(str(value), safe="")


def _base_uri(url: str) -> str:
    """URI de base signée : schéma et hôte en minuscules, sans port par
    défaut, sans query string ni fragment (RFC 5849 §3.4.1.2)."""
    parts = urllib.parse.urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = (parts.hostname or "").lower()
    if parts.port is not None and parts.port != _DEFAULT_PORTS.get(scheme):
        netloc = f"{netloc}:{parts.port}"
    return urllib.parse.urlunsplit((scheme, netloc, parts.path, "", ""))


def _collect_params(url, body, content_type, oauth_params) -> list:
    """Paramètres entrant dans la signature : query string + paramètres OAuth,
    plus le corps s'il est en form-urlencoded. Un corps JSON n'est jamais
    signé (RFC 5849 §3.4.1.3.1) — c'est le cas des requêtes SuiteQL."""
    parts = urllib.parse.urlsplit(url)
    collected = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    if body and content_type and FORM_CONTENT_TYPE in content_type.lower():
        if isinstance(body, bytes):
            body = body.decode("utf-8", "replace")
        collected += urllib.parse.parse_qsl(body, keep_blank_values=True)
    return collected + list(oauth_params.items())


def _normalize(params) -> str:
    """Tri lexicographique sur les paires *déjà encodées*, puis concaténation
    (RFC 5849 §3.4.1.3.2)."""
    encoded = sorted((_quote(key), _quote(value)) for key, value in params)
    return "&".join(f"{key}={value}" for key, value in encoded)


class OAuth1Signer(AuthBase):
    """Signe chaque requête sortante avec un header `Authorization: OAuth …`.

    `realm` est requis par NetSuite (l'identifiant de compte, ex. `1234567` ou
    `1234567_SB1`) ; il est transmis dans le header mais n'entre pas dans la
    signature.
    """

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        token: str | None = None,
        token_secret: str | None = None,
        realm: str | None = None,
        signature_method: str = "HMAC-SHA256",
    ):
        if signature_method not in SIGNATURE_METHODS:
            known = ", ".join(sorted(SIGNATURE_METHODS))
            raise ValueError(
                f"oauth1 : 'signature_method' inconnue '{signature_method}' — "
                f"attendu l'un de : {known}"
            )
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.token = token
        self.token_secret = token_secret
        self.realm = realm
        self.signature_method = signature_method

    def _oauth_params(self) -> dict:
        params = {
            "oauth_consumer_key": self.consumer_key,
            "oauth_nonce": secrets.token_hex(16),
            "oauth_signature_method": self.signature_method,
            "oauth_timestamp": str(int(time.time())),
            "oauth_version": "1.0",
        }
        if self.token:
            params["oauth_token"] = self.token
        return params

    def signature_base_string(self, method, url, body, content_type, oauth_params) -> str:
        return "&".join(
            [
                method.upper(),
                _quote(_base_uri(url)),
                _quote(_normalize(_collect_params(url, body, content_type, oauth_params))),
            ]
        )

    def sign(self, method, url, body, content_type, oauth_params) -> str:
        base_string = self.signature_base_string(
            method, url, body, content_type, oauth_params
        )
        signing_key = f"{_quote(self.consumer_secret)}&{_quote(self.token_secret or '')}"
        digest = hmac.new(
            signing_key.encode(),
            base_string.encode(),
            SIGNATURE_METHODS[self.signature_method],
        ).digest()
        return base64.b64encode(digest).decode()

    def authorization_header(self, method, url, body=None, content_type=None) -> str:
        oauth_params = self._oauth_params()
        oauth_params["oauth_signature"] = self.sign(
            method, url, body, content_type, oauth_params
        )
        pairs = [f'{_quote(k)}="{_quote(v)}"' for k, v in sorted(oauth_params.items())]
        if self.realm:
            pairs.insert(0, f'realm="{self.realm}"')
        return "OAuth " + ", ".join(pairs)

    def __call__(self, request):
        request.headers["Authorization"] = self.authorization_header(
            request.method,
            request.url,
            request.body,
            request.headers.get("Content-Type"),
        )
        return request
