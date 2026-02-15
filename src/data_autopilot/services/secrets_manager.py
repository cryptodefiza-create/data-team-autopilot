from __future__ import annotations

import base64
import json


class SecretsManager:
    """Local encryption helper."""

    def encrypt(self, payload: dict) -> dict:
        raw = json.dumps(payload).encode("utf-8")
        return {"ciphertext": base64.b64encode(raw).decode("utf-8"), "scheme": "base64-demo"}

    def decrypt(self, encrypted: dict) -> dict:
        raw = base64.b64decode(encrypted.get("ciphertext", "").encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
