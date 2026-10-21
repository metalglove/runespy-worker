import asyncio
import json
import importlib.util
import sys
import unittest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

from runespy_worker import client, crypto  # noqa: E402
from runespy_worker.protocol import build_message  # noqa: E402


class AssignmentProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_and_error_echo_the_assignment_generation(self):
        for error in (None, "PROFILE_PRIVATE"):
            with self.subTest(error=error):
                queue = asyncio.Queue()
                task = {"task_id": "task", "username": "alice", "attempt_count": 7}
                context = AsyncMock()
                with patch.object(client.httpx, "AsyncClient", return_value=context), \
                     patch.object(client, "fetch_profile", AsyncMock(return_value=({}, error))), \
                     patch.object(client, "fetch_hiscores", AsyncMock(return_value=(None, error))):
                    await client.process_task(queue, task, "worker", None, None,
                                              asyncio.Semaphore(1), AsyncMock(), 0)
                result = queue.get_nowait()
                self.assertEqual(result["attempt_count"], 7)
                self.assertEqual(result["task_id"], "task")
                self.assertEqual(result["status"], "error" if error else "success")

    def test_identity_challenge_requires_the_private_key(self):
        private = Ed25519PrivateKey.generate()
        nonce = "12" * 32
        signature = crypto.sign_challenge(private, nonce)
        private.public_key().verify(bytes.fromhex(signature), bytes.fromhex(nonce))
        self.assertFalse(hasattr(crypto, "decrypt_secret"))

    def test_session_envelope_does_not_need_a_shared_secret(self):
        message = json.loads(build_message("auth", {"signature": "proof"}, "worker"))
        self.assertEqual(message["payload"], {"signature": "proof"})
        self.assertNotIn("hmac", message)

    @unittest.skipUnless(importlib.util.find_spec("flask"), "Install worker dependencies to test the web UI")
    def test_web_setup_and_start_use_identity_without_a_secret(self):
        from runespy_worker import webui

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "worker_id").write_text("registered-worker")
            (home / "worker_key.pem").write_text("test-key")
            start = Mock()
            with patch.object(webui, "RUNE_HOME", home), \
                 patch.object(webui, "_get_commit_status", return_value={}), \
                 patch.object(webui, "_fetch_webshare_stats", return_value=None), \
                 patch.object(webui, "_is_running", return_value=False), \
                 patch.object(webui, "_start_worker", start):
                http = webui.app.test_client()
                page = http.get("/")
                self.assertEqual(page.status_code, 200)
                self.assertNotIn(b"Save shared secret", page.data)
                self.assertEqual(http.post("/run-worker").status_code, 302)
                start.assert_called_once()
                self.assertEqual(http.post("/save-secret").status_code, 404)

    def test_cli_no_longer_offers_publicly_reversible_secret_exchange(self):
        from click.testing import CliRunner
        from runespy_worker.cli import main

        help_result = CliRunner().invoke(main, ["--help"])
        self.assertEqual(help_result.exit_code, 0)
        self.assertNotIn("save-secret", help_result.output)


if __name__ == "__main__":
    unittest.main()
