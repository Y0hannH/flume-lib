"""Assainissement des URL qui finissent dans un message d'erreur.

`error_message` est persisté dans la table Delta `log_runs`, lisible par tout
le lakehouse — un public plus large que le fichier de configuration. Une URL
complète y recopierait ses query params, et donc un secret qu'une config mal
écrite y aurait placé.

Deux mécanismes, parce qu'il y a deux façons pour une URL d'entrer dans un
message. `safe_url` couvre les messages que la lib construit elle-même. Mais
une `ConnectionError` est formée par urllib3, avant que le code de la lib
reprenne la main, et y recopie la cible demandée telle quelle :

    HTTPSConnectionPool(host='api.example.com', port=443): Max retries
    exceeded with url: /v1/items?api_key=SUPERSECRET&page=1 (Caused by ...)

D'où `scrub_query`, qui retire la query string d'un message déjà rédigé.
"""

from urllib.parse import urlencode, urlsplit, urlunsplit

REDACTED = "<redacted>"


def safe_url(url: str) -> str:
    """URL sans query string, pour les messages d'erreur."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def scrub_query(message: str, *urls: str | None) -> str:
    """Remplace dans `message` la query string de chacune des `urls` fournies.

    Plusieurs URL parce que celle qu'on demande et celle que requests a
    réellement préparée diffèrent dès qu'un `params=` s'y ajoute : c'est la
    seconde qui apparaît dans le message, la première qu'on a sous la main.
    """
    for url in urls:
        if not url:
            continue
        query = urlsplit(url).query
        if query and query in message:
            message = message.replace(query, REDACTED)
    return message


def effective_url(url: str, params=None) -> str:
    """URL telle que requests la construira, `params=` compris.

    L'URL demandée et celle qui apparaît dans le message d'erreur diffèrent :
    requests concatène `params` au moment de préparer la requête. Quand
    l'échec survient avant cette préparation, `exc.request` est absent et
    c'est la seule façon de retrouver la query string à masquer.
    """
    if not params:
        return url
    separator = "&" if urlsplit(url).query else "?"
    return f"{url}{separator}{urlencode(params, doseq=True)}"


def sanitized_request_error(exc: Exception, url: str, params=None) -> Exception:
    """Reconstruit une exception de requests avec un message débarrassé de sa
    query string. Le type est conservé : c'est lui qui décide du rejeu
    (`ConnectionError` et `Timeout` sont rejouables) et c'est lui qui est
    recopié dans `error_message`."""
    prepared = getattr(getattr(exc, "request", None), "url", None)
    message = scrub_query(str(exc), prepared, effective_url(url, params), url)
    if message == str(exc):
        return exc
    # `RequestException.__init__` accepte *args : toute la famille se
    # reconstruit ainsi. Si une sous-classe exotique refusait, l'erreur qui
    # remonterait serait la sienne, pas l'URL qu'on cherche à masquer.
    return type(exc)(message)
