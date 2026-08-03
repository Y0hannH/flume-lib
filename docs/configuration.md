# Référence de configuration

Référence exhaustive de toutes les options : la config JSON d'une source (clé par clé, avec obligatoire/optionnel et valeurs par défaut), les paramètres de `run_source`, et le format des références de secret.

## Vue d'ensemble d'une source

```json
{
  "name": "ma_source",
  "base_url": "https://api.exemple.com/v1/items",
  "params": {"statut": "actif"},
  "auth": { ... },
  "pagination": { ... },
  "incremental": { ... },
  "target_schema": "bronze",
  "target_table": "ma_source",
  "retry": { ... },
  "timeout_seconds": 60
}
```

| Clé | Type | Requis | Défaut | Description |
|---|---|---|---|---|
| `name` | string | non | `<sans_nom>` | Identifiant de la source — utilisé dans `watermark`, `log_runs` et `RunResult`. Fortement recommandé. |
| `base_url` | string | **oui** | — | URL de l'endpoint de données. |
| `params` | object | non | `{}` | Query params fixes ajoutés à chaque requête (ex. filtres). |
| `auth` | object | non | aucune auth | Voir [Auth](#auth). Absent ou `{"type": "none"}` = requêtes sans authentification. |
| `pagination` | object | non | appel unique | Voir [Pagination](#pagination). Absent ou `{"type": "none"}` = un seul appel HTTP. |
| `incremental` | object | non | désactivé | Voir [Incrémental](#incrémental-watermark). |
| `target_schema` | string | **oui** | — | Schéma de destination des données (lakehouse avec schémas obligatoire). |
| `target_table` | string | **oui** | — | Table de destination. Écriture en mode `append`, schéma de table fusionné (`schema_mode=merge`). |
| `retry` | object | non | voir [Retry](#retry) | Politique de retry HTTP. |
| `timeout_seconds` | number | non | `60` | Timeout de chaque requête HTTP de données. |

## Paramètres de `run_source`

```python
run_source(config, lakehouse_tables_path=..., storage_options=..., log_schema=...)
```

| Paramètre | Type | Défaut | Description |
|---|---|---|---|
| `config` | dict | — | Une entrée du JSON de conf (voir ci-dessus). |
| `lakehouse_tables_path` | str | `/lakehouse/default/Tables` | Racine des tables. Dans Fabric, le défaut est automatiquement résolu vers l'URI ABFSS OneLake du lakehouse par défaut du notebook. Peut être une URI `abfss://...` explicite pour cibler un autre lakehouse. |
| `storage_options` | dict \| None | `None` | Passé tel quel à delta-rs. Si absent et URI `abfss://`, un bearer token de stockage est obtenu via `notebookutils`. |
| `log_schema` | str | `flume` | Schéma des tables techniques `watermark` et `log_runs`. |

Retour : `RunResult` avec `source_name`, `status` (`success`/`failed`), `rows_loaded`, `error_message` (None si succès), `start_ts`, `end_ts` (ISO 8601 UTC), `run_id` (UUID). **Ne lève jamais d'exception.**

## Références de secret

Partout où une valeur est marquée *réf. secret*, trois formes sont acceptées :

| Forme | Exemple | Usage |
|---|---|---|
| Variable d'environnement | `{"env_var": "MON_TOKEN"}` | Valeur injectée avant l'appel (os.environ). |
| Azure Key Vault | `{"keyvault_url": "https://monkv.vault.azure.net", "secret_name": "mon-secret"}` | Résolu via `notebookutils.credentials.getSecret` dans Fabric (identité du notebook — permission *Get* sur les secrets requise), ou `azure-identity`/`DefaultAzureCredential` hors Fabric (extra `pip install flume-lib[azure]`). |
| Littéral | `"valeur"` | **Uniquement pour les valeurs non sensibles** (username public, `grant_type`, scope…). Jamais un mot de passe ou un token. |

Les valeurs résolues sont automatiquement débarrassées des espaces et retours à la ligne en début/fin (source classique de 401).

Forme historique toujours supportée : `token_env_var`, `key_env_var`, `username_env_var`, `password_env_var` (équivalent à `{"env_var": ...}` sur la clé correspondante).

## Auth

Le type est sélectionné par `auth.type`. Le token est obtenu **une fois par run** (pas de refresh en cours de run).

### `bearer_token` — token statique

| Clé | Requis | Défaut | Description |
|---|---|---|---|
| `token` | **oui** | — | Réf. secret du token. |
| `header_name` | non | `Authorization` | Header qui porte le token. |
| `value_prefix` | non | `Bearer ` (avec espace) | Préfixe devant le token. `""` pour envoyer le token brut. |

```json
{"type": "bearer_token", "token": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "api-token"}}
```

Avec header custom (API qui attend le token brut dans un header spécifique) :

```json
{"type": "bearer_token", "token": {"env_var": "TOKEN"}, "header_name": "X-Access-Token", "value_prefix": ""}
```

### `api_key_header` — clé d'API dans un header

| Clé | Requis | Défaut | Description |
|---|---|---|---|
| `key` | **oui** | — | Réf. secret de la clé. |
| `header_name` | non | `X-API-Key` | Header qui porte la clé (ex. `Ocp-Apim-Subscription-Key`). |

```json
{"type": "api_key_header", "key": {"env_var": "MA_CLE"}, "header_name": "Ocp-Apim-Subscription-Key"}
```

### `basic` — HTTP Basic

| Clé | Requis | Défaut | Description |
|---|---|---|---|
| `username` | **oui** | — | Réf. secret (ou littéral si non sensible). |
| `password` | **oui** | — | Réf. secret. |

```json
{"type": "basic", "username": "svc_flume", "password": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "pwd"}}
```

### `oauth2_client_credentials` — flux OAuth2 standard (service principal Entra ID inclus)

Envoie `grant_type=client_credentials` + `client_id` + `client_secret` (+ `scope` si fourni) en **form-encoded** sur le `token_url`, et attend le token sous la clé `access_token` de la réponse JSON. Si votre endpoint dévie de ce standard (autre nom de clé, body JSON…), utilisez `token_endpoint`.

| Clé | Requis | Défaut | Description |
|---|---|---|---|
| `token_url` | oui, sauf si `tenant_id` | — | URL du token endpoint de l'IdP. |
| `tenant_id` | oui, sauf si `token_url` | — | Raccourci Entra ID : déduit `token_url = https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token`. |
| `client_id` | **oui** | — | Réf. secret (ou littéral — un app id Entra n'est pas un secret). |
| `client_secret` | **oui** | — | Réf. secret. |
| `scope` | non | absent | Ex. `https://graph.microsoft.com/.default` (Entra) ou un scope propriétaire. |
| `timeout_seconds` | non | `30` | Timeout de l'appel token. |

Service principal sur API Microsoft :

```json
{
  "type": "oauth2_client_credentials",
  "tenant_id": "00000000-0000-0000-0000-000000000000",
  "client_id": "11111111-1111-1111-1111-111111111111",
  "client_secret": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "sp-secret"},
  "scope": "https://graph.microsoft.com/.default"
}
```

IdP non-Microsoft avec scope propriétaire :

```json
{
  "type": "oauth2_client_credentials",
  "token_url": "https://auth.exemple.com/token",
  "client_id": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "cid"},
  "client_secret": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "csecret"},
  "scope": "monitoring"
}
```

### `token_endpoint` — login API arbitraire

Pour toute API où le token s'obtient par un appel préalable non standard : POST d'un login/password, body JSON, token imbriqué dans la réponse, header de sortie custom…

| Clé | Requis | Défaut | Description |
|---|---|---|---|
| `token_url` | **oui** | — | URL de l'appel d'obtention du token. |
| `method` | non | `POST` | Méthode HTTP. En `GET`, le `body` part en query params. |
| `body` | non | `{}` | Dict clé → valeur ; chaque valeur est un littéral **ou** une réf. secret, mélange libre. |
| `body_format` | non | `json` | `json` (body JSON) ou `form` (form-encoded). |
| `headers` | non | `{}` | Headers de l'appel token ; mêmes règles que `body`. |
| `token_json_path` | non | `access_token` | Chemin pointé du token dans la réponse JSON (ex. `data.token`, `result.auth.jwt`). |
| `header_name` | non | `Authorization` | Header utilisé ensuite sur les appels de données. |
| `value_prefix` | non | `Bearer ` | Préfixe du token dans ce header. |
| `timeout_seconds` | non | `30` | Timeout de l'appel token. |

Login/password (les deux en Key Vault), token imbriqué :

```json
{
  "type": "token_endpoint",
  "token_url": "https://api.exemple.com/login",
  "body": {
    "username": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "api-user"},
    "password": {"keyvault_url": "https://kv.vault.azure.net", "secret_name": "api-pwd"}
  },
  "token_json_path": "data.token"
}
```

Variante form-encoded avec header de sortie custom :

```json
{
  "type": "token_endpoint",
  "token_url": "https://api.exemple.com/token",
  "body_format": "form",
  "body": {"api_key": {"env_var": "K"}, "grant_type": "client_credentials"},
  "token_json_path": "token",
  "header_name": "X-Auth-Token",
  "value_prefix": ""
}
```

### `none` / absent

Aucun header d'auth. `{"type": "none"}`, `{}` ou clé `auth` absente.

## Pagination

Le type est sélectionné par `pagination.type`. Toutes les stratégies acceptent :

| Clé commune | Défaut | Description |
|---|---|---|
| `items_field` | auto | Champ de la réponse contenant la liste d'enregistrements. À défaut : réponse-liste détectée telle quelle, sinon recherche de `data`, `items`, `results`, `value`. Erreur explicite si introuvable. |

### `offset` — offset/limit

| Clé | Défaut | Description |
|---|---|---|
| `limit` | `100` | Taille de page demandée. |
| `limit_param` | `limit` | Nom du query param de taille (ex. `top`). |
| `offset_param` | `offset` | Nom du query param de décalage (ex. `skip`). |

Arrêt : page vide ou page partielle (`< limit`).

```json
{"type": "offset", "limit": 500, "limit_param": "top", "offset_param": "skip"}
```

### `page` — numéro de page (total en header supporté)

| Clé | Défaut | Description |
|---|---|---|
| `page_param` | `page` | Nom du query param du numéro de page. |
| `start_page` | `1` | Premier numéro (mettre `0` pour les APIs qui comptent depuis 0). |
| `size_param` | absent | Nom du query param de taille de page ; envoyé seulement si `page_size` aussi présent. |
| `page_size` | absent | Taille de page demandée. |
| `total_pages_header` | absent | Header de réponse contenant le nombre total de pages (ex. `X-Total-Pages`). Lu sur la première réponse ; erreur explicite s'il est absent ou non numérique. |

Arrêt : après `total_pages` pages si `total_pages_header` est renseigné ; sinon page vide, ou partielle si `page_size` connu.

```json
{"type": "page", "page_param": "page", "size_param": "per_page", "page_size": 100, "total_pages_header": "X-Total-Pages"}
```

### `next_link` — URL de page suivante dans la réponse

| Clé | Défaut | Description |
|---|---|---|
| `next_field` | `next` | Champ de la réponse contenant l'URL complète de la page suivante (ex. `@odata.nextLink` pour les APIs Microsoft). |

Les query params de `params`/`incremental` ne sont envoyés que sur le premier appel — l'URL suivante embarque déjà les siens. Arrêt : champ absent ou `null`.

```json
{"type": "next_link", "next_field": "@odata.nextLink", "items_field": "value"}
```

### `cursor` — non implémenté (stub)

Lève `NotImplementedError` (le run sort en `failed` avec ce message).

### `none` / absent

Un seul appel HTTP, sans boucle.

## Incrémental (watermark)

| Clé | Requis | Description |
|---|---|---|
| `enabled` | non (défaut `false`) | Active le mode incrémental. |
| `field` | oui si enabled | Champ des enregistrements dont le **max** devient le nouveau watermark. |
| `param_name` | oui si enabled | Query param envoyé à l'API avec le dernier watermark (ex. `updated_since`). |

Comportement : lecture du dernier watermark dans `<log_schema>.watermark` en début de run (aucun param envoyé au premier run) ; écriture du nouveau watermark **seulement si le run est un succès** et qu'au moins un enregistrement a été chargé. Comparaison par `max()` Python — fonctionne pour les timestamps ISO 8601 et les numériques ; attention aux formats de date non triables lexicographiquement.

## Retry

| Clé | Défaut | Description |
|---|---|---|
| `max_attempts` | `3` | Nombre total de tentatives par requête. |
| `backoff_multiplier` | `1` | Multiplicateur du backoff exponentiel (tenacity `wait_exponential`). |

Rejoué : erreurs réseau (connexion, timeout), HTTP 429 et 5xx. **Non rejoué** : les autres 4xx (401, 403, 404…) échouent immédiatement. S'applique aux appels de données ; l'appel token (`oauth2_client_credentials`/`token_endpoint`) n'est pas rejoué.

## Tables techniques

Créées automatiquement au premier run dans `<log_schema>` (défaut `flume`) :

| Table | Colonnes |
|---|---|
| `watermark` | `source_name`, `last_value`, `updated_ts` — une ligne ajoutée par avancement (la valeur courante est le max de `updated_ts` par source). |
| `log_runs` | `run_id`, `source_name`, `start_ts`, `end_ts`, `status`, `rows_loaded`, `error_message` — une ligne par appel à `run_source`, succès ou échec. |
