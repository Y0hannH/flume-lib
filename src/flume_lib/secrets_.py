"""Résolution de références de secrets. Un secret n'est jamais en clair dans
la config : il est référencé soit par variable d'environnement, soit par un
secret Azure Key Vault.

Formes acceptées pour une référence :
- {"env_var": "NOM_VAR"} — lu depuis os.environ
- {"keyvault_url": "https://monkv.vault.azure.net", "secret_name": "mon-secret"}
  — lu via notebookutils (Fabric) ou, à défaut, azure-identity +
  azure-keyvault-secrets (extra [azure])
- une chaîne littérale — réservée aux valeurs non sensibles (grant_type,
  username public…), jamais pour un token ou mot de passe
"""

import os
import threading
from urllib.parse import quote, quote_plus

from flume_lib.urls_ import REDACTED

# En deçà, une « valeur secrète » est un mot courant : la masquer détruirait
# les messages d'erreur sans protéger quoi que ce soit.
MIN_REDACTABLE_LENGTH = 6

_known_secrets: set[str] = set()
_known_lock = threading.Lock()


class SecretResolutionError(Exception):
    pass


def register_secret(value) -> None:
    """Mémorise une valeur secrète résolue, pour la masquer ensuite dans les
    messages (`redact`). La lib est la seule à connaître ces valeurs : c'est ce
    qui lui permet de masquer un secret qu'une API recopierait dans une
    réponse d'erreur, là où aucune règle sur la *forme* du message ne le
    pourrait. Les formes encodées sont mémorisées aussi — un secret renvoyé
    dans une URL y arrive percent-encodé."""
    if not isinstance(value, str) or len(value) < MIN_REDACTABLE_LENGTH:
        return
    variants = {value, quote(value, safe=""), quote_plus(value)}
    with _known_lock:
        _known_secrets.update(v for v in variants if len(v) >= MIN_REDACTABLE_LENGTH)


def redact(text: str) -> str:
    """Remplace par `<redacted>` toute valeur secrète mémorisée présente dans
    `text`. Les plus longues d'abord : un secret qui en contient un autre doit
    être masqué en entier, pas amputé."""
    if not text:
        return text
    with _known_lock:
        known = sorted(_known_secrets, key=len, reverse=True)
    for secret in known:
        if secret in text:
            text = text.replace(secret, REDACTED)
    return text


def forget_secrets() -> None:
    """Vide la mémoire des secrets. Réservé aux tests : en production la liste
    est bornée par le nombre de credentials distincts d'un processus."""
    with _known_lock:
        _known_secrets.clear()


def _get_keyvault_secret(vault_url: str, secret_name: str) -> str:
    try:
        import notebookutils  # préinstallé dans les notebooks Fabric

        return notebookutils.credentials.getSecret(vault_url, secret_name)
    except ImportError:
        pass

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as exc:
        raise SecretResolutionError(
            "Key Vault access outside Fabric: install flume-lib[azure] "
            "(azure-identity + azure-keyvault-secrets)"
        ) from exc

    client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
    return client.get_secret(secret_name).value


def resolve_secret(ref, field_name: str = "secret") -> str:
    """Résout une référence de secret vers sa valeur.

    field_name n'apparaît que dans les messages d'erreur, jamais la valeur.
    """
    # strip : un secret copié avec un espace ou retour à la ligne final
    # produit des 401 difficiles à diagnostiquer
    if isinstance(ref, str):
        return ref.strip()

    if isinstance(ref, dict):
        if "env_var" in ref:
            value = os.environ.get(ref["env_var"])
            if value is None:
                raise SecretResolutionError(
                    f"'{field_name}': environment variable "
                    f"'{ref['env_var']}' is not set"
                )
            value = value.strip()
            register_secret(value)
            return value

        if "keyvault_url" in ref:
            secret_name = ref.get("secret_name")
            if not secret_name:
                raise SecretResolutionError(
                    f"'{field_name}': 'secret_name' missing from the Key Vault "
                    "reference"
                )
            value = _get_keyvault_secret(ref["keyvault_url"], secret_name).strip()
            register_secret(value)
            return value

    raise SecretResolutionError(
        f"'{field_name}': invalid reference — expected a string, "
        "{'env_var': ...} or {'keyvault_url': ..., 'secret_name': ...}"
    )
