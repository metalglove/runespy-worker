import sys
import unittest
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from runespy_worker.fetcher import fetch_profile

N_SKILLS = 29


def hiscores(unranked=()):
    skills = [{"id": 0, "name": "Overall", "rank": 1234567, "level": 2000, "xp": 290_000_000}]
    for sid in range(1, N_SKILLS + 1):
        if sid in unranked:
            skills.append({"id": sid, "name": f"S{sid}", "rank": -1, "level": -1, "xp": -1})
        else:
            skills.append({"id": sid, "name": f"S{sid}", "rank": 100 + sid, "level": 99, "xp": 10_000_000})
    return {"name": "le me", "skills": skills, "activities": []}


EVENTS = {"name": "Le Me", "events": [
    {"type": "rs_plog_item_drop", "date": "2026-11-05T10:12:34Z", "text": "I found a Seismic wand",
     "details": "After killing Vorago, it dropped a Seismic wand."},
]}
QUESTS = {"name": "Le Me", "qp": 1, "qp_max": 476, "quests": {
    "257": {"title": "Cook's Assistant", "status": "COMPLETED", "qp": 1},
    "258": {"title": "Demon Slayer", "status": "NOT_STARTED", "qp": 3},
}}


def client(responses):
    """responses: {"hiscores"|"events"|"quests": (status, json)}"""
    def handler(request):
        assert request.headers["User-Agent"].startswith("RuneSpy-worker/")
        path = request.url.path
        key = "hiscores" if path.endswith("index_lite.json") else "events" if "/playerEvents/" in path else "quests"
        if key == "hiscores":
            assert request.url.params["player"] == "le me"
        else:
            assert path.endswith("/le me")
        status, body = responses.get(key, (200, {"hiscores": hiscores(), "events": EVENTS, "quests": QUESTS}[key]))
        return httpx.Response(status, json=body)
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class FetcherTests(unittest.IsolatedAsyncioTestCase):
    async def fetch(self, **responses):
        async with client(responses) as c:
            return await fetch_profile(c, "le+me")

    async def test_profile_shape(self):
        data, error = await self.fetch()
        self.assertIsNone(error)
        self.assertEqual(data["_source"], "platform")
        self.assertEqual(data["name"], "Le Me")
        self.assertEqual(data["rank"], "1,234,567")
        self.assertEqual(data["totalxp"], 290_000_000)
        self.assertEqual([s["id"] for s in data["skillvalues"]], list(range(N_SKILLS)))
        self.assertEqual(data["skillvalues"][0], {"id": 0, "level": 99, "xp": 100_000_000, "rank": 101})
        self.assertEqual(data["activities"][0]["date"], "2026-11-05T10:12:34Z")
        self.assertEqual((data["questscomplete"], data["questsstarted"], data["questsnotstarted"]), (1, 0, 1))

    async def test_unranked_skill_keeps_negative_xp(self):
        data, _ = await self.fetch(hiscores=(200, hiscores(unranked={6})))
        self.assertEqual(data["skillvalues"][5]["xp"], -1)

    async def test_not_on_hiscores_is_private(self):
        self.assertEqual(await self.fetch(hiscores=(404, {"error": "PLAYER_NOT_FOUND"})), (None, "PROFILE_PRIVATE"))

    async def test_non_member_and_hidden_events_are_empty(self):
        for status, code in ((403, "PLAYER_NOT_MEMBER"), (404, "PLAYER_NOT_FOUND")):
            data, error = await self.fetch(events=(status, {"error": code}))
            self.assertIsNone(error)
            self.assertEqual(data["activities"], [])

    async def test_events_outage_is_an_api_error(self):
        self.assertEqual(await self.fetch(events=(500, {"error": "INTERNAL_ERROR"})), (None, "API_ERROR"))

    async def test_quests_failure_is_best_effort(self):
        data, error = await self.fetch(quests=(500, {"error": "INTERNAL_ERROR"}))
        self.assertIsNone(error)
        self.assertNotIn("questscomplete", data)

    async def test_display_name_prefers_events_then_quests(self):
        data, _ = await self.fetch(quests=(500, {"error": "INTERNAL_ERROR"}))
        self.assertEqual(data["name"], "Le Me")
        data, _ = await self.fetch(events=(403, {"error": "PLAYER_NOT_MEMBER"}),
                                   quests=(500, {"error": "INTERNAL_ERROR"}))
        self.assertEqual(data["name"], "le me")

    async def test_rate_limit(self):
        self.assertEqual(await self.fetch(hiscores=(429, {})), (None, "RATE_LIMITED"))
        self.assertEqual(await self.fetch(quests=(429, {})), (None, "RATE_LIMITED"))


if __name__ == "__main__":
    unittest.main()
