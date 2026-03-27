"""RuneMetrics profile fetcher — standalone copy for the worker client."""

import httpx

_PRIVATE_ERRORS = {
    "not a member", "not_a_member",
    "profile is private", "profile_private",
    "no profile", "no_profile",
}


async def fetch_profile(
    client: httpx.AsyncClient, username: str,
) -> tuple[dict | None, str | None]:
    """Fetch a player profile from the RuneMetrics API.

    Returns (data, error) — exactly one will be non-None.
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
            if any(p in error_msg for p in _PRIVATE_ERRORS):
                return None, "PROFILE_PRIVATE"
            return None, "API_ERROR"
        return data, None
    except httpx.TimeoutException:
        return None, "TIMEOUT"
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
    """Fallback: fetch basic skill data from the hiscores lite API."""
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
    except (httpx.HTTPError, ValueError, IndexError):
        return None, "API_ERROR"
