"""Les exemples disent vrai.

Ils servent de documentation qu'on copie : un exemple dont la config ne valide
plus, ou une option que plus aucun exemple ne montre, est une documentation qui
a menti sans que personne ne s'en aperçoive. Deux contrôles :

- chaque config construite par un exemple passe `validate_config` ;
- chaque clé de configuration et chaque fonction publique apparaît dans au
  moins un exemple.
"""

import importlib.util
import inspect
from pathlib import Path

import pytest

import flume_lib
from flume_lib import validation as V

EXAMPLES = sorted((Path(__file__).resolve().parent.parent / "examples").glob("*.py"))

# Cellules de notebook qui lisent un fichier du lakehouse dès l'import : on les
# lit comme du texte (clés, fonctions, version), on ne les exécute pas.
SCRIPTS = {"notebook_ingest_example.py"}

# Montre des blocs `auth`, pas des configs de source complètes.
AUTH_ONLY = {"rest_api_auth_variants.py"}

# Configs volontairement refusées, pour montrer le message de refus.
REFUSED_ON_PURPOSE = {"orders_refused"}


def load(path: Path):
    spec = importlib.util.spec_from_file_location(f"example_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configs_of(module) -> list[dict]:
    """Les configs complètes d'un exemple : dicts (ou listes de dicts) de premier
    niveau qui ont un nom, une URL et une cible. Les fragments (`BASE`, `AUTH`)
    n'en sont pas."""
    found = []
    for value in vars(module).values():
        for item in value if isinstance(value, list) else [value]:
            if (
                isinstance(item, dict)
                and {"name", "base_url", "target_table"} <= item.keys()
            ):
                found.append(item)
    return found


def test_there_are_examples():
    assert len(EXAMPLES) >= 8


@pytest.mark.parametrize(
    "path", [p for p in EXAMPLES if p.name not in SCRIPTS], ids=lambda p: p.name
)
def test_every_config_an_example_builds_validates(path):
    module = load(path)
    for config in configs_of(module):
        if config["name"] in REFUSED_ON_PURPOSE:
            with pytest.raises(V.ConfigError):
                V.validate_config(config)
            continue
        try:
            V.validate_config(config)
        except V.ConfigError as exc:  # pragma: no cover - message de diagnostic
            pytest.fail(f"{path.name}: '{config['name']}' no longer validates: {exc}")


def test_the_examples_that_are_checked_do_build_configs():
    """Un exemple dont plus aucune config n'est reconnue ne vérifierait rien."""
    for path in EXAMPLES:
        if path.name not in SCRIPTS | AUTH_ONLY:
            assert configs_of(load(path)), path.name


def test_every_example_pins_the_current_version():
    for path in EXAMPLES:
        text = path.read_text(encoding="utf-8")
        if "flume-lib==" in text:
            assert f"flume-lib=={flume_lib.__version__}" in text, path.name


TEXT = "\n".join(p.read_text(encoding="utf-8") for p in EXAMPLES)


def _key_groups():
    groups = {
        "top-level": V._OPTIONAL,
        "retry": V._RETRY_KEYS,
        "write": V._WRITE_KEYS,
        "incremental": V._INCREMENTAL_KEYS,
        "incremental.window": V._INCREMENTAL_WINDOW_KEYS,
    }
    for auth_type, keys in V._AUTH_KEYS.items():
        groups[f"auth.{auth_type}"] = keys
    return groups


def test_every_configuration_key_appears_in_an_example():
    missing = [
        f"{group}: {key}"
        for group, keys in _key_groups().items()
        for key in keys
        if f'"{key}"' not in TEXT
    ]
    assert missing == [], "keys that no example shows: " + ", ".join(missing)


def test_every_public_function_appears_in_an_example():
    functions = [
        name for name in flume_lib.__all__
        if inspect.isfunction(getattr(flume_lib, name))
    ]
    missing = [name for name in functions if name not in TEXT]
    assert missing == [], "functions that no example calls: " + ", ".join(missing)


def test_every_run_source_parameter_appears_in_an_example():
    parameters = [
        name for name in inspect.signature(flume_lib.run_source).parameters
        if name != "config"
    ]
    missing = [name for name in parameters if f"{name}=" not in TEXT]
    assert missing == [], "run_source parameters that no example uses: " + ", ".join(missing)
