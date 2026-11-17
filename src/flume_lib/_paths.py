"""Chemins pointés dans du JSON (`data.orders.pageInfo.endCursor`).

Une seule implémentation pour la pagination, l'auth et le templating : quatre
parcours écrits à la main divergeaient sur ce qu'est une étape manquante."""

# Distingue « chemin absent » de « chemin présent valant None » : hasNextPage
# à False et hasNextPage absent n'ont pas le même sens, le second est une
# réponse qu'on ne sait pas interpréter.
MISSING = object()


def get_path(payload, path: str):
    """Descend `path` dans `payload`. Retourne `MISSING` si une étape n'existe
    pas, ou si l'on doit traverser autre chose qu'un objet."""
    value = payload
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return MISSING
        value = value[part]
    return value
