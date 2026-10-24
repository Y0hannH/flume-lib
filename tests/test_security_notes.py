"""Les notes de sécurité sont vérifiées contre ce qu'elles décrivent.

docs/security.md affirme des choses vérifiables : des plafonds chiffrés, des
actions épinglées par SHA, des lockfiles à hashes, une release signée, un audit
des versions figées. Une note de sécurité périmée est pire qu'une absence de
note — on s'y fie. Chaque chiffre ci-dessous est lu dans le code ; chaque
affirmation sur le dépôt est lue dans le dépôt.
"""

import importlib.util
import re
from pathlib import Path

import pytest

from flume_lib import locks, secrets_, templating
from flume_lib import source as source_module

ROOT = Path(__file__).resolve().parent.parent
SECURITY = (ROOT / "docs" / "security.md").read_text(encoding="utf-8")
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def spaced(number: int) -> str:
    """2000 -> « 2 000 », comme la doc écrit les milliers."""
    return f"{number:,}".replace(",", " ")


class TestNumbersMatchTheCode:
    def test_the_message_cap(self):
        assert f"{spaced(source_module.MAX_RUN_MESSAGE_CHARS)} characters" in SECURITY

    def test_the_shortest_value_that_is_masked(self):
        assert f"shorter than {secrets_.MIN_REDACTABLE_LENGTH} characters" in SECURITY

    def test_the_response_cap(self):
        mib = source_module.DEFAULT_MAX_RESPONSE_BYTES // (1024 * 1024)
        assert f"{mib} MiB" in SECURITY

    def test_the_quoted_error_text(self):
        assert f"{source_module.MAX_ERROR_DETAIL_CHARS} characters" in SECURITY
        assert f"first {source_module.MAX_ERRORS_REPORTED} errors" in SECURITY

    def test_the_part_of_a_4xx_body_that_is_read(self):
        kib = source_module._ERROR_BODY_READ_BYTES // 1024
        assert f"{kib} KiB" in SECURITY

    def test_the_retry_after_ceiling(self):
        assert f"default {source_module.DEFAULT_MAX_RETRY_AFTER_SECONDS} s" in SECURITY

    def test_the_timeouts(self):
        assert f"default {source_module.DEFAULT_TIMEOUT_SECONDS} s" in SECURITY
        from flume_lib.auth import DEFAULT_TOKEN_TIMEOUT_SECONDS

        assert f"default {DEFAULT_TOKEN_TIMEOUT_SECONDS} s" in SECURITY

    def test_every_forbidden_character_is_listed(self):
        """La note énumère ce qu'un watermark interpolé ne peut pas contenir."""
        listed = {"'", '"', ";", "--", "/*", "\\", "\n", "\r", "\x00"}
        assert set(templating._FORBIDDEN_TOKENS) == listed
        for phrase in ("a backslash", "newline", "carriage return", "NUL byte"):
            assert phrase in SECURITY

    def test_the_lock_default_lifetime_is_not_misquoted(self):
        assert locks.DEFAULT_LOCK_TTL_SECONDS == 6 * 3600


class TestWorkflowsMatchTheNotes:
    def steps(self):
        for path in WORKFLOWS:
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                match = re.match(r"\s*(?:-\s*)?uses:\s*(\S+)", line)
                if match:
                    yield path.name, number, match.group(1)

    def test_there_are_steps_to_check(self):
        assert sum(1 for _ in self.steps()) >= 10

    def test_every_action_is_pinned_to_a_commit_sha(self):
        mutable = [
            f"{name}:{number} {ref}"
            for name, number, ref in self.steps()
            if not ref.startswith("./") and not re.search(r"@[0-9a-f]{40}$", ref)
        ]
        assert not mutable, f"not pinned by SHA: {mutable}"

    def test_the_release_is_attested(self):
        release = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
        assert "actions/attest-build-provenance@" in release
        assert re.search(r"id-token:\s*write", release)
        assert re.search(r"attestations:\s*write", release)

    def test_the_release_builds_with_hashed_tools(self):
        release = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
        assert "--require-hashes -r requirements/build.txt" in release
        assert "--no-isolation" in release
        assert "pip install build\n" not in release

    def test_the_weekly_audit_covers_the_locked_versions(self):
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert "audit_dependencies.py --requirements requirements/runtime.txt" in ci
        assert re.search(r'cron:\s*"0 6 \* \* 1"', ci)  # lundi 06:00 UTC

    def test_the_release_audits_each_bundle(self):
        release = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
        assert "audit_dependencies.py --wheels fabric-wheels" in release
        assert "verify_wheels.py" in release

    def test_dependabot_watches_what_the_notes_say(self):
        dependabot = (ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8")
        assert 'package-ecosystem: github-actions' in dependabot
        assert 'directory: "/requirements"' in dependabot


class TestLockfilesAreHashed:
    @pytest.mark.parametrize("name", ["runtime.txt", "build.txt"])
    def test_every_pinned_package_carries_hashes(self, name):
        text = (ROOT / "requirements" / name).read_text(encoding="utf-8")
        stanzas = re.split(r"\n(?=[A-Za-z0-9][A-Za-z0-9._-]*==)", text)
        pinned = [s for s in stanzas if re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*==", s)]
        assert len(pinned) >= 5
        bare = [s.splitlines()[0] for s in pinned if "--hash=sha256:" not in s]
        assert not bare, f"{name}: no hash for {bare}"


# ---------------------------------------------------------------------------
# audit_dependencies.py --requirements
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def audit():
    path = ROOT / "scripts" / "audit_dependencies.py"
    spec = importlib.util.spec_from_file_location("audit_dependencies", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestRequirementsAudit:
    LOCK = (
        "arro3-core==0.8.3 ; python_full_version < '3.11' \\\n"
        "    --hash=sha256:aaaa\n"
        "arro3-core==0.9.1 ; python_full_version >= '3.11' \\\n"
        "    --hash=sha256:bbbb\n"
        "Typing_Extensions==4.16.0 \\\n"
        "    --hash=sha256:cccc\n"
        "    # via deprecated\n"
        "requests==2.34.2 \\\n"
        "    --hash=sha256:dddd\n"
    )

    def test_every_pinned_version_is_audited_including_per_kernel_ones(self, audit, tmp_path):
        lock = tmp_path / "lock.txt"
        lock.write_text(self.LOCK, encoding="utf-8")
        assert audit.requirements_closure(lock) == [
            ("arro3-core", "0.8.3"),
            ("arro3-core", "0.9.1"),
            ("requests", "2.34.2"),
            ("typing-extensions", "4.16.0"),
        ]

    def test_comments_and_hash_lines_are_not_packages(self, audit, tmp_path):
        lock = tmp_path / "lock.txt"
        lock.write_text("# comment\n    --hash=sha256:ff\n    # via x\n", encoding="utf-8")
        assert audit.requirements_closure(lock) == []

    def test_the_real_lockfile_yields_its_packages(self, audit):
        closure = audit.requirements_closure(ROOT / "requirements" / "runtime.txt")
        names = {name for name, _ in closure}
        assert {"deltalake", "arro3-core", "requests", "tenacity", "urllib3"} <= names

    def test_a_missing_lockfile_is_exit_2_not_a_pass(self, audit, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["audit", "--requirements", "does-not-exist.txt"])
        assert audit.main() == 2
        assert "introuvable" in capsys.readouterr().out

    def test_the_modes_are_exclusive(self, audit, monkeypatch):
        monkeypatch.setattr(
            "sys.argv", ["audit", "--requirements", "a.txt", "--wheels", "b"]
        )
        with pytest.raises(SystemExit):
            audit.main()

    def test_a_vulnerable_pin_is_reported_as_exit_1(self, audit, monkeypatch, capsys, tmp_path):
        lock = tmp_path / "lock.txt"
        lock.write_text(self.LOCK, encoding="utf-8")
        monkeypatch.setattr("sys.argv", ["audit", "--requirements", str(lock)])
        monkeypatch.setattr(
            audit,
            "query_osv",
            lambda packages: [["GHSA-xxxx"] if n == "requests" else [] for n, _ in packages],
        )
        monkeypatch.setattr(audit, "describe", lambda identifier: "test advisory")
        assert audit.main() == 1
        assert "requests 2.34.2" in capsys.readouterr().out

    def test_an_unreachable_osv_is_exit_2(self, audit, monkeypatch, tmp_path):
        import urllib.error

        lock = tmp_path / "lock.txt"
        lock.write_text(self.LOCK, encoding="utf-8")
        monkeypatch.setattr("sys.argv", ["audit", "--requirements", str(lock)])

        def unreachable(packages):
            raise urllib.error.URLError("no network")

        monkeypatch.setattr(audit, "query_osv", unreachable)
        assert audit.main() == 2


# ---------------------------------------------------------------------------
# La note ne se contredit pas
# ---------------------------------------------------------------------------


class TestTheNoteDoesNotContradictItself:
    def test_the_count_of_returning_values_is_consistent(self):
        assert "Four values" in SECURITY
        assert "Three things" not in SECURITY

    def test_the_next_link_risk_is_described_as_bounded(self):
        assert "Only the API's own honesty" not in SECURITY
        assert "origin of `base_url`" in SECURITY

    def test_the_offline_bundle_is_described_as_locked_not_resolved(self):
        assert "resolved at release time" not in SECURITY
