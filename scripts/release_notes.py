"""Extrait la section d'une version du CHANGELOG, pour servir de notes de
release.

Le CHANGELOG est déjà la description de référence d'une version : la recopier
à la main dans l'interface GitHub, c'est se garantir deux textes qui divergent.

Usage :
    python scripts/release_notes.py v0.10.0
    python scripts/release_notes.py 0.10.0 --output NOTES.md

Sort en 1 si la version n'a pas de section — une release sans notes est une
release qu'on publie sans savoir ce qu'elle contient.

Sans dépendance : stdlib seule.
"""

import argparse
import pathlib
import re
import sys

CHANGELOG = pathlib.Path(__file__).resolve().parent.parent / "CHANGELOG.md"


def extract(changelog: str, version: str) -> str | None:
    """Contenu de la section `## [version]`, jusqu'au titre de même niveau."""
    escaped = re.escape(version)
    match = re.search(
        rf"^## \[{escaped}\][^\n]*\n(.*?)(?=^## |\Z)",
        changelog,
        re.MULTILINE | re.DOTALL,
    )
    return match.group(1).strip() if match else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("version", help="Version ou tag (v0.10.0 ou 0.10.0)")
    parser.add_argument("--output", help="Écrire dans ce fichier au lieu de stdout")
    parser.add_argument(
        "--changelog", default=str(CHANGELOG), help="Chemin du CHANGELOG"
    )
    args = parser.parse_args()

    version = args.version.removeprefix("v")
    path = pathlib.Path(args.changelog)
    if not path.exists():
        print(f"CHANGELOG introuvable : {path}", file=sys.stderr)
        return 1

    notes = extract(path.read_text(encoding="utf-8"), version)
    if not notes:
        print(
            f"Aucune section '## [{version}]' dans {path.name} — "
            "ajouter la section avant de publier la release.",
            file=sys.stderr,
        )
        return 1

    if args.output:
        pathlib.Path(args.output).write_text(notes + "\n", encoding="utf-8", newline="\n")
        print(f"{len(notes.splitlines())} ligne(s) écrites dans {args.output}")
    else:
        print(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
