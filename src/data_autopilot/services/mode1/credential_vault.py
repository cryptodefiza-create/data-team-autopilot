from __future__ import annotations

import base64
import json
import logging
from typing import Any
from uuid import uuid4

from data_autopilot.services.mode1.models import CredentialRecord

logger = logging.getLogger(__name__)


class CredentialVault:
    """Encrypted storage for user API keys and tokens.

    Mock mode uses base64 encoding (NOT real encryption).
    Production would use AES-256-GCM with a KMS-managed key.
    """

    def __init__(self, mock_mode: bool = True) -> None:
        self._mock_mode = mock_mode
        self._store: dict[str, dict[str, str]] = {}  # org_id -> {source -> encrypted}
        self._records: dict[str, CredentialRecord] = {}  # cred_id -> record

    def store(self, org_id: str, source: str, credentials: dict[str, Any]) -> str:
        encrypted = self._encrypt(json.dumps(credentials))
        cred_id = f"cred_{uuid4().hex[:10]}"

        if org_id not in self._store:
            self._store[org_id] = {}
        self._store[org_id][source] = encrypted

        record = CredentialRecord(
            id=cred_id,
            org_id=org_id,
            source=source,
            validated=True,
        )
        self._records[cred_id] = record
        logger.info("Stored credentials for org %s source %s", org_id, source)
        return cred_id

    def retrieve(self, org_id: str, source: str) -> dict[str, Any] | None:
        org_store = self._store.get(org_id, {})
        encrypted = org_store.get(source)
        if encrypted is None:
            return None
        return json.loads(self._decrypt(encrypted))

    def has_credentials(self, org_id: str, source: str) -> bool:
        return source in self._store.get(org_id, {})

    def get_record(self, cred_id: str) -> CredentialRecord | None:
        return self._records.get(cred_id)

    def purge(self, org_id: str) -> int:
        """Delete all credentials for an org. Returns count deleted."""
        count = 0
        self._store.pop(org_id, None)
        to_remove = [k for k, v in self._records.items() if v.org_id == org_id]
        for k in to_remove:
            del self._records[k]
            count += 1
        logger.info("Purged %d credentials for org %s", count, org_id)
        return count

    def is_encrypted(self, org_id: str, source: str) -> bool:
        """Check if stored value is not plaintext JSON."""
        raw = self._store.get(org_id, {}).get(source, "")
        if not raw:
            return False
        try:
            json.loads(raw)
            return False  # If it parses as JSON directly, it's not encrypted
        except (json.JSONDecodeError, ValueError):
            return True

    def _encrypt(self, plaintext: str) -> str:
        if self._mock_mode:
            return base64.b64encode(plaintext.encode()).decode()
        raise NotImplementedError("Production encryption not yet implemented")

    def _decrypt(self, ciphertext: str) -> str:
        if self._mock_mode:
            return base64.b64decode(ciphertext.encode()).decode()
        raise NotImplementedError("Production decryption not yet implemented")
