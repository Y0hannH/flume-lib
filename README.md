# flume-lib

Accélérateur d'ingestion API générique pour notebooks **Microsoft Fabric Python** (non-Spark). Python pur : écriture Delta via [delta-rs](https://github.com/delta-io/delta-rs) (`deltalake`), sans dépendance à PySpark.

## Installation

### Recommandé : wheels dans le lakehouse (hors-ligne, sans GitHub ni PyPI au runtime)

Générer le lot de wheels (lib + dépendances, pour le kernel Fabric Python 3.12 / Linux) :

```
python scripts/build_fabric_wheels.py
```

Uploader les `.whl` du dossier `fabric-wheels/` dans `Files/libs` du lakehouse, puis dans le notebook :

```
%pip install --no-index --find-links=/lakehouse/default/Files/libs flume-lib
```

`--no-index` garantit que rien n'est téléchargé au runtime : le code exécuté est exactement celui déposé dans le lakehouse. Ne jamais écraser un wheel existant — une nouvelle version = un nouveau fichier. Autre kernel : `--python-version 3.11`.

### Alternative : install directe depuis GitHub

Épingler le **commit SHA complet** de la release plutôt que le tag : un tag peut être re-pointé par un attaquant ayant un accès en écriture au repo, un SHA est immuable.

```
%pip install git+https://github.com/Y0hannH/flume-lib.git@75fb767dad8c453930ad5249c2b4540c5f263ce0
```

| Version | SHA |
|---|---|
| v0.4.0 | `75fb767dad8c453930ad5249c2b4540c5f263ce0` |

Le SHA d'une version se retrouve avec `git rev-parse vX.Y.Z`, ou sur la [page des tags](https://github.com/Y0hannH/flume-lib/tags).

## Usage

```python
from flume_lib import run_source
import json

with open("/lakehouse/default/Files/conf/sources.json") as f:
    sources = json.load(f)

for source_config in sources:
    result = run_source(source_config)
    print(f"{source_config['name']}: {result.status} ({result.rows_loaded} lignes)")
```

`run_source(config, lakehouse_tables_path="/lakehouse/default/Tables")` ne lève **jamais** d'exception : toute erreur est catchée et remontée dans le `RunResult` (`status`, `rows_loaded`, `error_message`, `start_ts`, `end_ts`, `run_id`), pour que la boucle appelante continue sur la source suivante.

La lib cible exclusivement des **lakehouses avec schémas** : chaque source déclare son schéma de destination (`target_schema`, requis — ex. `bronze`), et les tables techniques (`watermark`, `log_runs`) vont dans un schéma dédié, `flume` par défaut, personnalisable via `run_source(..., log_schema="...")`.

## Configuration d'une source

```json
{
  "name": "source_exemple",
  "base_url": "https://api.exemple.com/v1/items",
  "auth": {
    "type": "bearer_token",
    "token_env_var": "SOURCE_EXEMPLE_TOKEN"
  },
  "pagination": {
    "type": "offset",
    "limit": 100,
    "limit_param": "limit",
    "offset_param": "offset"
  },
  "incremental": {
    "enabled": true,
    "field": "updated_at",
    "param_name": "updated_since"
  },
  "target_schema": "bronze",
  "target_table": "source_exemple",
  "retry": {
    "max_attempts": 3,
    "backoff_multiplier": 1
  }
}
```

### Auth

Les credentials ne sont **jamais** en clair dans la config. Chaque credential est une **référence de secret**, résolue au runtime :

- `{"env_var": "NOM_VAR"}` — variable d'environnement
- `{"keyvault_url": "https://monkv.vault.azure.net", "secret_name": "mon-secret"}` — Azure Key Vault, via `notebookutils` dans Fabric (préinstallé), ou `flume-lib[azure]` hors Fabric
- une chaîne littérale — **uniquement** pour les valeurs non sensibles (username public, `grant_type`…)

La forme historique `token_env_var` / `key_env_var` / `username_env_var` / `password_env_var` reste supportée.

| Type | Clés de config | Statut |
|---|---|---|
| `bearer_token` | `token` (réf. secret), `header_name` (défaut `Authorization`), `value_prefix` (défaut `Bearer `) | ✅ |
| `api_key_header` | `key` (réf. secret), `header_name` (défaut `X-API-Key`) | ✅ |
| `basic` | `username`, `password` (réf. secret) | ✅ |
| `oauth2_client_credentials` | `tenant_id` ou `token_url`, `client_id`, `client_secret` (réf. secret), `scope` | ✅ |
| `token_endpoint` | `token_url`, `method` (défaut `POST`), `body`, `body_format` (`json`/`form`), `headers`, `token_json_path` (défaut `access_token`), `header_name`, `value_prefix` | ✅ |

**Service principal Entra ID (APIs Microsoft : Graph, Fabric, Azure Management…)** — `oauth2_client_credentials` avec `tenant_id` (le `token_url` `login.microsoftonline.com/.../oauth2/v2.0/token` est déduit) :

```json
"auth": {
  "type": "oauth2_client_credentials",
  "tenant_id": "00000000-0000-0000-0000-000000000000",
  "client_id": "11111111-1111-1111-1111-111111111111",
  "client_secret": {"keyvault_url": "https://monkv.vault.azure.net", "secret_name": "sp-flume-secret"},
  "scope": "https://graph.microsoft.com/.default"
}
```

**Token obtenu via un appel API de login** — `token_endpoint` ; les valeurs de `body`/`headers` sont des littéraux ou des références de secret, le token est extrait de la réponse JSON par chemin pointé :

```json
"auth": {
  "type": "token_endpoint",
  "token_url": "https://api.exemple.com/login",
  "body": {
    "username": "svc_flume",
    "password": {"keyvault_url": "https://monkv.vault.azure.net", "secret_name": "api-exemple-pwd"}
  },
  "token_json_path": "data.token"
}
```

Le token est obtenu une fois par `run_source` (pas de refresh en cours de run).

### Pagination

| Type | Clés de config | Statut |
|---|---|---|
| `offset` | `limit`, `limit_param`, `offset_param` | ✅ |
| `page` | `page_param` (défaut `page`), `start_page` (défaut 1), `size_param` + `page_size` (optionnels), `total_pages_header` (optionnel) | ✅ |
| `next_link` | `next_field` (défaut `next`), `items_field` | ✅ |
| `cursor` | — | stub |

`items_field` (optionnel, toutes stratégies) : nom du champ de la réponse contenant les enregistrements. À défaut, la lib détecte une réponse liste, ou cherche `data` / `items` / `results` / `value`.

**`page` avec total en header** : si `total_pages_header` est renseigné (ex. `"X-Total-Pages"`), le nombre total de pages est lu dans les headers de la première réponse et toutes les pages sont parcourues. Sans ce header, arrêt sur page vide (ou partielle si `page_size` est renseigné) :

```json
"pagination": {
  "type": "page",
  "page_param": "page",
  "size_param": "per_page",
  "page_size": 100,
  "total_pages_header": "X-Total-Pages"
}
```

### Incrémental (watermark)

Si `incremental.enabled`, le dernier watermark est lu dans la table `watermark` et passé en query param (`param_name`). En fin de run **réussi uniquement**, le max de `incremental.field` sur les enregistrements chargés devient le nouveau watermark. Pas d'avancement du watermark en cas d'échec.

### Retry

Backoff exponentiel via `tenacity` sur les erreurs réseau et HTTP 429/5xx, paramétré par `retry.max_attempts` (défaut 3) et `retry.backoff_multiplier` (défaut 1). Les 4xx (hors 429) échouent immédiatement.

## Écriture Delta dans Fabric (OneLake)

Le montage local `/lakehouse/default/...` des notebooks Fabric ne supporte pas le rename atomique requis par le commit du transaction log delta-rs (`Operation not permitted (os error 1)`, table sans `_delta_log` valide). La lib contourne le problème automatiquement : dans Fabric, le chemin par défaut `/lakehouse/default/Tables` est résolu vers l'URI ABFSS OneLake du lakehouse par défaut du notebook, et l'écriture s'authentifie avec un token de stockage obtenu via `notebookutils` — rien à configurer.

Pour cibler un autre lakehouse, passer directement son URI ABFSS :

```python
run_source(config, lakehouse_tables_path="abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables")
```

Hors Fabric (stockage Azure ou local), `storage_options` est transmis tel quel à delta-rs : `run_source(config, lakehouse_tables_path=..., storage_options={...})`.

## Tables techniques

Tables Delta créées automatiquement dans le schéma technique (`log_schema`, défaut `flume`) du lakehouse :

- **`flume.watermark`** : `source_name`, `last_value`, `updated_ts`
- **`flume.log_runs`** : `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — une ligne par appel à `run_source`, succès ou échec

## Développement

```
pip install -e ".[dev]"
pytest
```

Tests unitaires mockés, aucun appel réseau réel.

## Hors scope

- CLI d'installation/scaffolding côté client
- Pagination cursor (stub)
