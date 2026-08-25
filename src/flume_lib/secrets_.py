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


class SecretResolutionError(Exception):
    pass


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
            return value.strip()

        if "keyvault_url" in ref:
            secret_name = ref.get("secret_name")
            if not secret_name:
                raise SecretResolutionError(
                    f"'{field_name}': 'secret_name' missing from the Key Vault "
                    "reference"
                )
            return _get_keyvault_secret(ref["keyvault_url"], secret_name).strip()

    raise SecretResolutionError(
        f"'{field_name}': invalid reference — expected a string, "
        "{'env_var': ...} or {'keyvault_url': ..., 'secret_name': ...}"
    )
