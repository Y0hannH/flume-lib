"""Vérifie que le tableau des SHA du README couvre bien les versions publiées.

L'installation depuis GitHub épingle un SHA de commit, jamais un tag : un tag
peut être redirigé par quelqu'un qui a le droit d'écrire sur le dépôt, un SHA
non. Le tableau du README est donc ce qui rend cette forme d'installation
utilisable — une version absente du tableau est une version que personne ne
peut installer sans aller la chercher dans l'historique git.

Il a été oublié deux fois de suite (v0.10.1 et v0.10.2 publiées, tableau resté
à v0.10.0), ce qui est le sort réservé à toute étape de release confiée à la
mémoire. D'où ce contrôle.

**Un commit ne peut pas contenir son propre SHA** : la ligne d'une version est
forcément ajoutée par un commit postérieur au tag. Le contrôle en tient compte
et n'exige une ligne que pour les tags **strictement antérieurs** au commit
courant. Sur le tag lui-même, il ne demande rien.

Les versions antérieures à la plus ancienne ligne du tableau sont ignorées :
le tableau documente les versions installables, pas la préhistoire du dépôt.

Usage :
    python scripts/check_readme_shas.py

Sort en 1 en listant les lignes à ajouter, telles qu'elles doivent l'être.

Sans dépendance : stdlib seule.
"""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
README = ROOT / "README.md"

TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
ROW_RE = re.compile(r"^\|\s*(v\d+\.\d+\.\d+)\s*\|\s*`([0-9a-f]{40})`\s*\|", re.M)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.strip()


def version_key(tag: str) -> tuple[int, int, int] | None:
    match = TAG_RE.match(tag)
    return tuple(int(part) for part in match.groups()) if match else None


def published_tags() -> dict[str, str]:
    """Tags vX.Y.Z du dépôt, associés au commit qu'ils désignent."""
    tags = {}
    for tag in git("tag", "--list", "v*").splitlines():
        tag = tag.strip()
        if version_key(tag) is None:
            continue
        tags[tag] = git("rev-list", "-n1", tag)
    return tags


def is_strict_ancestor(commit: str, head: str) -> bool:
    """Le commit est-il dans l'histoire de HEAD, sans être HEAD lui-même ?

    C'est la question qui rend le contrôle utilisable sur le tag qu'on est en
    train de publier : ce tag pointe sur HEAD, sa ligne ne peut pas y être.
    """
    if commit == head:
        return False
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, head],
        cwd=ROOT,
        capture_output=True,
    )
    return result.returncode == 0


def main() -> int:
    documented = dict(ROW_RE.findall(README.read_text(encoding="utf-8")))
    if not documented:
        print("check_readme_shas : tableau des SHA introuvable dans le README.")
        return 1

    # Plancher : la plus ancienne version documentée. Rien avant elle n'est
    # réclamé — le tableau n'a jamais prétendu remonter au premier commit.
    floor = min(version_key(tag) for tag in documented)

    head = git("rev-parse", "HEAD")
    missing, wrong, pending = [], [], []
    for tag, commit in sorted(published_tags().items(), key=lambda kv: version_key(kv[0])):
        if version_key(tag) < floor:
            continue
        if commit == head and tag not in documented:
            # Le tag qu'on vient de poser. Sa ligne ne peut pas être dans ce
            # commit, mais c'est maintenant qu'on en a besoin : l'imprimer
            # sans faire échouer quoi que ce soit.
            pending.append((tag, commit))
            continue
        if not is_strict_ancestor(commit, head):
            continue
        if tag not in documented:
            missing.append((tag, commit))
        elif documented[tag] != commit:
            wrong.append((tag, documented[tag], commit))

    for tag, listed, actual in wrong:
        print(f"{tag} : le README annonce {listed}, le tag pointe sur {actual}")
    if missing:
        print("Versions publiées absentes du tableau des SHA du README :")
        for tag, commit in reversed(missing):
            print(f"| {tag} | `{commit}` |")
        print("\nÀ insérer en tête du tableau (les versions y descendent).")
    if missing or wrong:
        return 1

    print(f"README : {len(documented)} versions listées, tableau à jour.")
    for tag, commit in pending:
        print(
            f"\n{tag} vient d'être posé sur HEAD. Ligne à ajouter au tableau, "
            "dans un commit séparé :\n"
            f"| {tag} | `{commit}` |"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
