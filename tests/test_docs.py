"""La documentation est vérifiée comme du code.

Le cookbook des agents promet que chaque config qu'il donne est valide et que
chaque extrait de code appelle la bibliothèque comme elle existe. Un agent qui
recopie un exemple périmé ne le sait pas ; ce test le sait à sa place.

- chaque config complète (bloc ```json config) passe `validate_config` ;
- chaque bloc Python s'exécute, avec des doublures aux endroits qui feraient du
  réseau ou écriraient dans un lakehouse — mais avec la **vraie** signature :
  un argument qui n'existe pas lève ;
- chaque lien relatif et chaque ancre du README et de docs/ pointe quelque part ;
- les versions épinglées des lignes d'installation valent la version du paquet.
"""

import inspect
import json
import re
from pathlib import Path

import pytest

import flume_lib
from flume_lib import RunResult, validate_config

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
DOCS = sorted((ROOT / "docs").glob("*.md"))
AGENT_COOKBOOK = ROOT / "docs" / "agent-cookbook.md"

FENCE_RE = re.compile(r"^```(\S*)[ \t]*(\S*)[ \t]*\n(.*?)^```[ \t]*$", re.S | re.M)


def blocks(path: Path, language: str, tag: str | None = None) -> list[str]:
    return [
        body
        for lang, label, body in FENCE_RE.findall(path.read_text(encoding="utf-8"))
        if lang == language and (tag is None or label == tag)
    ]


# ---------------------------------------------------------------------------
# Configs du cookbook des agents
# ---------------------------------------------------------------------------

AGENT_CONFIGS = blocks(AGENT_COOKBOOK, "json", "config")


class TestAgentCookbookConfigs:
    def test_the_recipes_are_there(self):
        """Sans quoi les tests suivants passeraient à vide."""
        assert len(AGENT_CONFIGS) >= 14

    @pytest.mark.parametrize("raw", AGENT_CONFIGS, ids=lambda raw: json.loads(raw)["name"])
    def test_every_complete_config_validates(self, raw):
        validate_config(json.loads(raw))

    def test_every_config_has_a_name(self):
        """Règle 7 du cookbook : le watermark et `log_runs` sont indexés dessus."""
        assert all("name" in json.loads(raw) for raw in AGENT_CONFIGS)

    def test_names_are_unique(self):
        names = [json.loads(raw)["name"] for raw in AGENT_CONFIGS]
        assert len(names) == len(set(names))

    def test_no_recipe_carries_a_literal_secret(self):
        """Valider ne suffit pas : `allow_literal_secrets` aurait pu s'y glisser."""
        for raw in AGENT_CONFIGS:
            config = json.loads(raw)
            assert "allow_literal_secrets" not in config
            assert "allow_insecure_http" not in config


# ---------------------------------------------------------------------------
# Les extraits Python s'exécutent
# ---------------------------------------------------------------------------


@pytest.fixture
def fakes(monkeypatch):
    """Remplace ce qui fait du réseau ou écrit, en contrôlant la signature réelle."""
    calls = []

    def checked(name, behaviour):
        real = getattr(flume_lib, name)
        signature = inspect.signature(real)

        def wrapper(*args, **kwargs):
            signature.bind(*args, **kwargs)  # TypeError si l'appel n'existe pas
            calls.append(name)
            return behaviour(*args, **kwargs)

        monkeypatch.setattr(flume_lib, name, wrapper)

    def fake_run_source(config, *args, **kwargs):
        validate_config(config)
        return RunResult(
            source_name=config.get("name", "<sans_nom>"), status="success", rows_loaded=0,
            error_message=None, start_ts="", end_ts="", sample=[], warnings=[],
        )

    def fake_run_source_windows(config_factory, until, max_span_days, *args, **kwargs):
        validate_config(config_factory(str(until)))
        return []

    def fake_run_sources(configs, *args, **kwargs):
        return flume_lib.SourcesResult([fake_run_source(config) for config in configs])

    checked("run_source", fake_run_source)
    checked("run_sources", fake_run_sources)
    checked("run_source_windows", fake_run_source_windows)
    checked("read_watermark", lambda *a, **k: None)
    checked("write_watermark", lambda *a, **k: None)
    checked("reset_watermark", lambda *a, **k: 0)
    checked("compact_technical_tables", lambda *a, **k: {})
    return calls


def run_blocks(path: Path, fakes_calls) -> dict:
    namespace: dict = {"__name__": "__doc_example__"}
    for source in blocks(path, "python"):
        exec(compile(source, f"{path.name} (python block)", "exec"), namespace)  # noqa: S102
    return namespace


class TestPythonBlocks:
    def test_agent_cookbook_blocks_run_against_the_real_signatures(self, fakes):
        namespace = run_blocks(AGENT_COOKBOOK, fakes)
        assert "run_source" in fakes  # le squelette a bien appelé la bibliothèque
        assert "run_sources" in fakes
        assert "run_source_windows" in fakes
        assert "compact_technical_tables" in fakes
        # le squelette valide ses propres sources avant de les lancer
        assert namespace["SOURCES"]

    def test_readme_example_runs(self, fakes):
        run_blocks(README, fakes)
        assert "run_source" in fakes

    def test_every_import_is_a_name_the_package_exports(self):
        for path in (README, *DOCS):
            for source in blocks(path, "python"):
                for statement in re.findall(
                    r"from flume_lib import (\([^)]*\)|[^\n]*)", source
                ):
                    names = [n.strip() for n in statement.strip("()").replace("\n", " ").split(",")]
                    for name in filter(None, names):
                        assert name in flume_lib.__all__, f"{path.name}: {name}"


# ---------------------------------------------------------------------------
# Liens et ancres
# ---------------------------------------------------------------------------

LINK_RE = re.compile(r"(?<!\!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")


def strip_fences(text: str) -> str:
    return FENCE_RE.sub("", text)


def slug(heading: str) -> str:
    """L'ancre que GitHub donne à un titre."""
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # [texte](url) -> texte
    text = text.replace("`", "").replace("*", "")
    text = re.sub(r"[^\w\- ]", "", text.lower())
    return text.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    seen: dict[str, int] = {}
    found = set()
    for line in strip_fences(path.read_text(encoding="utf-8")).splitlines():
        match = HEADING_RE.match(line)
        if not match:
            continue
        base = slug(match.group(2))
        count = seen.get(base, 0)
        seen[base] = count + 1
        found.add(base if count == 0 else f"{base}-{count}")
    return found


def links(path: Path):
    text = strip_fences(path.read_text(encoding="utf-8"))
    for target in LINK_RE.findall(text):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        yield target


class TestLinks:
    @pytest.mark.parametrize("path", [README, *DOCS], ids=lambda p: p.name)
    def test_relative_links_and_anchors_resolve(self, path):
        broken = []
        for target in links(path):
            file_part, _, fragment = target.partition("#")
            destination = path if not file_part else (path.parent / file_part).resolve()
            if not destination.exists():
                broken.append(f"{target}  (no such file)")
            elif fragment and destination.suffix == ".md" and fragment not in anchors(destination):
                broken.append(f"{target}  (no such heading)")
        assert not broken, f"{path.name}: " + "; ".join(broken)

    def test_slug_matches_what_github_does(self):
        assert slug("`merge` — keep the latest version of each object") == (
            "merge--keep-the-latest-version-of-each-object"
        )
        assert slug("4.5 Reload strategy") == "45-reload-strategy"
        assert slug("9. Key reference (generated)") == "9-key-reference-generated"


# ---------------------------------------------------------------------------
# Versions épinglées
# ---------------------------------------------------------------------------


class TestPinnedVersions:
    PIN_RE = re.compile(r"flume-lib==(\d+\.\d+\.\d+)")

    def test_every_install_line_names_the_current_version(self):
        # lu à la main : `tomllib` n'existe qu'à partir de Python 3.11
        version = re.search(
            r'^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text(encoding="utf-8"), re.M
        ).group(1)
        assert flume_lib.__version__ == version
        stale = {}
        for path in (README, *DOCS, *sorted((ROOT / "examples").glob("*.py"))):
            for pinned in self.PIN_RE.findall(path.read_text(encoding="utf-8")):
                if pinned != version:
                    stale[path.name] = pinned
        assert not stale, f"pinned to another version than {version}: {stale}"


# ---------------------------------------------------------------------------
# Ce que le README promet à son lecteur
# ---------------------------------------------------------------------------


class TestReadmeStaysHighLevel:
    def test_the_agent_link_is_at_the_top(self):
        head = "\n".join(README.read_text(encoding="utf-8").splitlines()[:6])
        assert "docs/agent-cookbook.md" in head

    def test_it_is_short(self):
        """Le README sert à décider, pas à configurer : le détail vit dans docs/.
        La borne est large exprès ; la dépasser est un signal, pas une règle."""
        assert len(README.read_text(encoding="utf-8").splitlines()) <= 120

    def test_every_technical_document_is_reachable_from_it(self):
        text = README.read_text(encoding="utf-8")
        for name in ("agent-cookbook", "cookbook", "configuration", "install", "security",
                     "development"):
            assert f"docs/{name}.md" in text, name


# ---------------------------------------------------------------------------
# Ce que la bibliothèque expose est documenté
# ---------------------------------------------------------------------------

CONFIGURATION = (ROOT / "docs" / "configuration.md").read_text(encoding="utf-8")
AGENT_TEXT = AGENT_COOKBOOK.read_text(encoding="utf-8")


class TestEverythingExposedIsDocumented:
    def test_every_exported_name_is_in_the_agent_cookbook(self):
        missing = [name for name in flume_lib.__all__ if name not in AGENT_TEXT]
        assert missing == [], f"exported but not in agent-cookbook.md: {missing}"

    def test_every_run_result_field_is_in_the_agent_cookbook(self):
        import dataclasses

        missing = [f.name for f in dataclasses.fields(RunResult) if f.name not in AGENT_TEXT]
        assert missing == [], f"RunResult fields missing from agent-cookbook.md: {missing}"

    def test_every_log_runs_column_is_in_the_configuration_reference(self):
        from flume_lib.logging_ import LOG_RUNS_DETAIL_TYPES

        missing = [c for c in LOG_RUNS_DETAIL_TYPES if f"`{c}`" not in CONFIGURATION]
        assert missing == [], f"log_runs columns missing from configuration.md: {missing}"

    def test_every_run_source_parameter_is_in_the_configuration_reference(self):
        signature = inspect.signature(flume_lib.run_source)
        parameters = [p for p in signature.parameters if p != "config"]
        missing = [p for p in parameters if f"`{p}`" not in CONFIGURATION]
        assert missing == [], f"run_source parameters missing from configuration.md: {missing}"

    def test_every_sources_result_member_is_in_the_agent_cookbook(self):
        members = [
            name for name, value in vars(flume_lib.SourcesResult).items()
            if not name.startswith("_") and (callable(value) or isinstance(value, property))
        ]
        missing = [m for m in members if m not in AGENT_TEXT]
        assert missing == [], f"SourcesResult members missing from agent-cookbook.md: {missing}"
