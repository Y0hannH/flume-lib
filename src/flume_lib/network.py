"""Réglages réseau d'un run : proxy sortant et autorité de certification.

Derrière un proxy d'entreprise à inspection TLS, un run échoue de deux façons
qui se ressemblent peu : le proxy n'est pas joint, ou il est joint et réémet les
certificats avec sa propre autorité, que `requests` ne connaît pas. Les deux se
règlent dans la config, sans toucher à l'environnement du notebook.

Ce qui n'existe pas, exprès : un interrupteur qui désactive la vérification des
certificats. Une CA d'entreprise (`ca_bundle`) règle le besoin réel ; couper la
vérification livrerait les credentials de l'API à quiconque se place sur le
chemin."""

import os
from urllib.parse import urlsplit

from flume_lib.secrets_ import SecretResolutionError, resolve_secret


class NetworkConfigError(Exception):
    """Un réglage réseau de la config ne peut pas être appliqué."""


def parse_proxy_url(url, allow_credentials: bool) -> None:
    """Lève ValueError si `url` n'est pas une URL de proxy utilisable. Le
    message ne recopie jamais l'URL : celle d'un proxy authentifié porte un
    mot de passe, et le message part dans log_runs."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError("must be a URL string")
    try:
        parts = urlsplit(url.strip())
        host = parts.hostname
        _ = parts.port  # lève ValueError sur un port illisible
    except ValueError:
        raise ValueError("is not a valid URL") from None
    if parts.scheme.lower() not in ("http", "https") or not host:
        raise ValueError("must be an absolute http:// or https:// URL, e.g. http://proxy.corp:3128")
    if "@" in parts.netloc and not allow_credentials:
        raise ValueError(
            "must not embed credentials (user:password@host) in the configuration — "
            'reference the whole URL as a secret instead: {"env_var": "NAME"} or '
            '{"keyvault_url": "...", "secret_name": "..."}'
        )


def resolve_network(config: dict) -> dict:
    """Arguments `requests` (`proxies`, `verify`) décrits par la config, ou un
    dict vide. Un proxy référencé comme secret est résolu ici, donc masqué dans
    les messages ensuite (voir `secrets_.redact`)."""
    settings: dict = {}

    proxy = config.get("proxy")
    if proxy is not None:
        referenced = isinstance(proxy, dict)
        try:
            url = resolve_secret(proxy, "proxy") if referenced else str(proxy).strip()
        except SecretResolutionError as exc:
            raise NetworkConfigError(str(exc)) from exc
        try:
            # une URL tirée d'un secret peut porter les identifiants du proxy
            parse_proxy_url(url, allow_credentials=referenced)
        except ValueError as exc:
            raise NetworkConfigError(f"proxy: {exc}") from None
        settings["proxies"] = {"http": url, "https": url}

    ca_bundle = config.get("ca_bundle")
    if ca_bundle is not None:
        if not os.path.isfile(ca_bundle):
            raise NetworkConfigError(
                f"ca_bundle: no file at '{ca_bundle}' — expected a PEM file holding the "
                "certificate authority to trust, e.g. in the lakehouse Files"
            )
        settings["verify"] = ca_bundle
    return settings
