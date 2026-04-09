"""RuneMetrics profile fetcher with hiscores fallback.

Primary source — RuneMetrics API
---------------------------------
``fetch_profile`` hits ``https://apps.runescape.com/runemetrics/profile/profile``
and returns the JSON blob directly.  The API returns ``{"error": "..."}`` for
private profiles and several other edge cases; the error message is compared
case-insensitively against ``_PRIVATE_ERRORS`` to distinguish a private profile
(``PROFILE_PRIVATE``) from a genuine API failure (``API_ERROR``).

Fallback — hiscores lite CSV
------------------------------
When RuneMetrics reports ``PROFILE_PRIVATE``, ``fetch_hiscores`` is tried.
Some players who hide their RuneMetrics profile still appear on the public
hiscores, providing at least their skill levels and XP.

The hiscores lite endpoint returns a plain-text CSV with one row per tracked
skill, plus an overall totals row prepended:

    Row 0   — Overall totals (rank, total_level, total_xp).  Stored separately;
              NOT added to ``skillvalues``.
    Row 1   — Skill 0 (Attack).  Becomes ``{"id": 0, ...}``.
    Row 2   — Skill 1 (Defence).  Becomes ``{"id": 1, ...}``.
    ...
    Row 29  — Skill 28 (Invention).  Becomes ``{"id": 28, ...}``.

RS3 tracks 29 skills (rows 1-29).  Only the first ``n_skills + 1`` rows are
parsed; activity rows that follow are ignored.

XP unit difference: the hiscores API returns XP values already divided by 10
(truncated to the nearest 0.1 XP).  To align with the RuneMetrics format (which
uses tenths of XP), every ``xp`` value is multiplied by 10 before being stored.

The synthesised result dict sets ``"_source": "hiscores"`` so the server can
distinguish it from a full RuneMetrics response and apply appropriate validation
rules (e.g. no ``activities`` field is present).
"""

import httpx

_BANNED_ERRORS = {
    "not a member", "not_a_member",
}

_PRIVATE_ERRORS = {
    "profile is private", "profile_private",
    "no profile", "no_profile",
}


async def fetch_profile(
    client: httpx.AsyncClient, username: str,
) -> tuple[dict | None, str | None]:
    """Fetch a player profile from the RuneMetrics API.

    Returns ``(data, None)`` on success or ``(None, error_code)`` on failure.
    Exactly one element of the tuple is non-None.

    Error codes:
      ``PROFILE_PRIVATE`` — API returned a privacy error; caller should try
                            ``fetch_hiscores`` as a fallback.
      ``NOT_A_MEMBER``    — player is banned; no fallback attempted.
      ``RATE_LIMITED``    — HTTP 429; caller should back off.
      ``TIMEOUT``         — request timed out after 15 s.
      ``API_ERROR``       — any other HTTP or parsing error.

    The ``+`` → ``" "`` substitution in *username* is needed because RuneScape
    usernames can contain spaces, which are sometimes stored URL-encoded.
    """
    url = "https://apps.runescape.com/runemetrics/profile/profile"
    api_user = username.replace("+", " ")
    params = {"user": api_user, "activities": 20}
    try:
        response = await client.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            error_msg = str(data["error"]).lower()
            if any(p in error_msg for p in _BANNED_ERRORS):
                return None, "NOT_A_MEMBER"
            if any(p in error_msg for p in _PRIVATE_ERRORS):
                return None, "PROFILE_PRIVATE"
            return None, "API_ERROR"
        return data, None
    except httpx.TimeoutException:
        return None, "TIMEOUT"
    except httpx.ProxyError:
        return None, "PROXY_ERROR"
    except httpx.ConnectError:
        return None, "API_ERROR"
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            return None, "RATE_LIMITED"
        return None, "API_ERROR"
    except httpx.HTTPError:
        return None, "API_ERROR"
    except ValueError:
        return None, "API_ERROR"


async def fetch_hiscores(
    client: httpx.AsyncClient, username: str,
) -> tuple[dict | None, str | None]:
    """Fetch basic skill data from the hiscores lite CSV endpoint.

    Used as a fallback when RuneMetrics reports ``PROFILE_PRIVATE``.  Some
    players who hide their RuneMetrics data are still visible on the hiscores.

    Returns ``(data, None)`` on success or ``(None, "API_ERROR")`` on failure.
    The returned ``data`` dict is shaped to match the RuneMetrics schema as
    closely as possible:

    - ``skillvalues`` — list of 29 dicts with ``id``, ``level``, ``xp`` (×10),
      and ``rank``.  Row 0 of the CSV is the overall totals row; it is stored
      as ``totalskill`` / ``totalxp`` and excluded from the per-skill list.
    - ``activities`` — always ``[]`` (hiscores provides no activity feed).
    - ``_source`` — set to ``"hiscores"`` so the server can apply appropriate
      validation rules for this reduced data set.

    HTML responses (redirects to a login/error page) are rejected so the caller
    always gets a clean ``API_ERROR`` rather than a parse exception.
    """
    api_user = username.replace("+", " ")
    url = f"https://secure.runescape.com/m=hiscore/index_lite.ws?player={api_user}"
    try:
        resp = await client.get(url, timeout=10, follow_redirects=True)
        if resp.status_code != 200:
            return None, "API_ERROR"
        body = resp.text.strip()
        if body.lower().startswith("<!doctype") or body.lower().startswith("<html"):
            return None, "API_ERROR"

        lines = body.split("\n")
        n_skills = 29  # RS3 has 29 skills
        skillvalues = []
        total_xp = 0
        total_level = 0
        for i, line in enumerate(lines[:n_skills + 1]):
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            rank, level, xp = int(parts[0]), int(parts[1]), int(parts[2])
            if i == 0:
                total_level = level
                total_xp = xp
                continue
            skill_id = i - 1
            skillvalues.append({
                "id": skill_id,
                "level": level,
                "xp": xp * 10,
                "rank": rank,
            })

        data = {
            "name": api_user,
            "skillvalues": skillvalues,
            "totalxp": total_xp,
            "totalskill": total_level,
            "activities": [],
            "_source": "hiscores",
        }
        return data, None
    except httpx.ProxyError:
        return None, "PROXY_ERROR"
    except (httpx.HTTPError, ValueError, IndexError):
        return None, "API_ERROR"
