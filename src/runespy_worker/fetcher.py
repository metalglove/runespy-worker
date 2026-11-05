"""Player profile fetcher for the RuneScape Platform API.

RuneMetrics was retired on 10 November 2026. Its profile data now comes from
three Platform API endpoints (https://developer.runescape.com/platform/):

    Skills, XP, ranks   -> Hiscores ``/m=hiscore/index_lite.json?player=NAME``
    Recent activities   -> Adventurer's Log ``/api/v1/playerEvents/NAME``
    Quest counts        -> Adventurer's Log ``/api/v1/playerQuests/NAME``

``fetch_profile`` calls all three and assembles a dict in the legacy
RuneMetrics profile shape so the master's validation and ingestion are
unchanged:

- ``skillvalues`` — 29 dicts ``{id, level, xp, rank}`` with RuneMetrics ids
  (hiscores ids minus one; 0 is Overall there) and XP x10 like RuneMetrics.
  Unranked skills keep ``xp: -1``.
- ``totalxp`` / ``totalskill`` / ``rank`` — from the hiscores Overall row.
- ``activities`` — the 20 most recent events with their raw ISO 8601 dates.
- ``questscomplete`` / ``questsstarted`` / ``questsnotstarted`` — counted from
  playerQuests; omitted if that call fails.
- ``_source: "platform"`` — tells the master to derive RuneMetrics-equivalent
  levels, combat level and legacy date strings before validating.

Error codes (worker protocol):
  ``PROFILE_PRIVATE`` — player not on the hiscores (unknown, hidden or banned).
  ``RATE_LIMITED``    — HTTP 429; caller should back off.
  ``TIMEOUT``         — a request timed out after 15 s.
  ``PROXY_ERROR``     — the proxy failed; caller may retry direct.
  ``API_ERROR``       — any other HTTP or parsing error.

``NOT_A_MEMBER`` is no longer produced: the events endpoint's
``PLAYER_NOT_MEMBER`` only means a free-to-play player (empty feed), not a ban.
"""

from urllib.parse import quote

import httpx

from runespy_worker import __version__

HISCORES_URL = "https://secure.runescape.com/m=hiscore/index_lite.json"
ADVENTURERS_LOG_URL = "https://secure.runescape.com/m=adventurers-log/api/v1"
# Jagex asks Platform API clients to identify themselves.
HEADERS = {"User-Agent": f"RuneSpy-worker/{__version__} (+https://runespy.com)"}
PLATFORM_SOURCE = "platform"


class _FetchError(Exception):
    def __init__(self, code: str, status: int | None = None, body: object = None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.body = body


def _api_name(username: str) -> str:
    """Stored usernames use ``+`` for spaces; the APIs want the display name."""
    return username.replace("+", " ")


def _player_url(endpoint: str, username: str) -> str:
    return f"{ADVENTURERS_LOG_URL}/{endpoint}/{quote(_api_name(username), safe='')}"


async def _get_json(client: httpx.AsyncClient, url: str, **kwargs) -> object:
    try:
        resp = await client.get(url, headers=HEADERS, timeout=15, **kwargs)
    except httpx.TimeoutException:
        raise _FetchError("TIMEOUT")
    except httpx.ProxyError:
        raise _FetchError("PROXY_ERROR")
    except httpx.HTTPError:
        raise _FetchError("API_ERROR")
    if resp.status_code == 429:
        raise _FetchError("RATE_LIMITED", 429)
    try:
        body = resp.json()
    except ValueError:
        body = None
    if resp.status_code >= 400:
        raise _FetchError("API_ERROR", resp.status_code, body)
    if body is None:
        raise _FetchError("API_ERROR", resp.status_code)
    return body


def _error_name(exc: _FetchError) -> str | None:
    return exc.body.get("error") if isinstance(exc.body, dict) else None


async def fetch_hiscores(client: httpx.AsyncClient, username: str) -> dict:
    try:
        body = await _get_json(client, HISCORES_URL, params={"player": _api_name(username)})
    except _FetchError as e:
        if e.status == 404:
            raise _FetchError("PROFILE_PRIVATE", 404) from e
        raise
    if not isinstance(body, dict) or not isinstance(body.get("skills"), list):
        raise _FetchError("API_ERROR")
    return body


async def fetch_events(client: httpx.AsyncClient, username: str) -> list:
    """Recent events; non-members and hidden players have an empty feed."""
    try:
        body = await _get_json(client, _player_url("playerEvents", username))
    except _FetchError as e:
        if e.status == 404 or _error_name(e) in ("PLAYER_NOT_MEMBER", "PLAYER_NOT_FOUND"):
            return []
        raise
    if not isinstance(body, dict) or not isinstance(body.get("events"), list):
        raise _FetchError("API_ERROR")
    return body["events"]


async def fetch_quests(client: httpx.AsyncClient, username: str) -> dict | None:
    """Quest statuses, best-effort: ``None`` on any non-rate-limit failure."""
    try:
        body = await _get_json(client, _player_url("playerQuests", username))
    except _FetchError as e:
        if e.code in ("RATE_LIMITED", "PROXY_ERROR"):
            raise
        return None
    if not isinstance(body, dict) or not isinstance(body.get("quests"), dict):
        return None
    return body


def build_profile(username: str, hiscores: dict, events: list, quests: dict | None) -> dict:
    """Assemble a RuneMetrics-shaped profile from Platform API responses."""
    skills = [s for s in hiscores.get("skills") or [] if isinstance(s, dict)]
    overall = next((s for s in skills if s.get("id") == 0), {})
    skillvalues = []
    for s in skills:
        sid = s.get("id")
        if type(sid) is not int or sid == 0:
            continue
        xp = s.get("xp")
        skillvalues.append({
            "id": sid - 1,
            "level": s.get("level"),
            "xp": xp * 10 if type(xp) is int and xp >= 0 else -1,
            "rank": s.get("rank"),
        })
    rank = overall.get("rank")
    data = {
        "name": hiscores.get("name") or _api_name(username),
        "skillvalues": skillvalues,
        "totalxp": overall.get("xp"),
        "totalskill": overall.get("level"),
        "rank": f"{rank:,}" if type(rank) is int and rank > 0 else None,
        "activities": [
            {k: e.get(k) for k in ("date", "text", "details", "type")}
            for e in events if isinstance(e, dict)
        ],
        "_source": PLATFORM_SOURCE,
    }
    if quests:
        statuses = [q.get("status") for q in quests["quests"].values() if isinstance(q, dict)]
        data["questscomplete"] = statuses.count("COMPLETED")
        data["questsstarted"] = statuses.count("STARTED")
        data["questsnotstarted"] = statuses.count("NOT_STARTED")
        if isinstance(quests.get("name"), str):
            data["name"] = quests["name"]
    return data


async def fetch_profile(
    client: httpx.AsyncClient, username: str,
) -> tuple[dict | None, str | None]:
    """Fetch a player profile. Returns ``(data, None)`` or ``(None, error_code)``."""
    try:
        hiscores = await fetch_hiscores(client, username)
        events = await fetch_events(client, username)
        quests = await fetch_quests(client, username)
    except _FetchError as e:
        return None, e.code
    return build_profile(username, hiscores, events, quests), None
