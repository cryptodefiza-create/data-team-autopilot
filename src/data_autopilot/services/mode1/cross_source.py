from __future__ import annotations

import logging
from typing import Any

from data_autopilot.services.mode1.models import CrossSourceResult

logger = logging.getLogger(__name__)


class CrossSourceJoin:
    """Joins public blockchain/API data with warehouse data.

    In mock mode, performs in-memory joins.
    Production would upload public data as a temp table in the warehouse
    and execute a SQL join.
    """

    def __init__(self) -> None:
        self._temp_tables: dict[str, list[dict[str, Any]]] = {}

    def join(
        self,
        public_data: list[dict[str, Any]],
        warehouse_data: list[dict[str, Any]],
        join_key: str,
        public_key: str | None = None,
        warehouse_key: str | None = None,
    ) -> CrossSourceResult:
        """Join public data with warehouse data on a common key.

        Args:
            public_data: Records from blockchain/API source
            warehouse_data: Records from warehouse (BigQuery/Postgres)
            join_key: Column name to join on (used for both if public_key/warehouse_key not set)
            public_key: Column name in public data (defaults to join_key)
            warehouse_key: Column name in warehouse data (defaults to join_key)
        """
        pk = public_key or join_key
        wk = warehouse_key or join_key

        # Build index on warehouse data
        warehouse_index: dict[Any, dict[str, Any]] = {}
        for record in warehouse_data:
            key = record.get(wk)
            if key is not None:
                warehouse_index[str(key).lower()] = record

        # Perform join
        joined: list[dict[str, Any]] = []
        for pub_record in public_data:
            key = pub_record.get(pk)
            if key is None:
                continue

            warehouse_match = warehouse_index.get(str(key).lower())
            if warehouse_match:
                merged = {**pub_record}
                for k, v in warehouse_match.items():
                    if k not in merged:
                        merged[k] = v
                    else:
                        merged[f"warehouse_{k}"] = v
                joined.append(merged)

        result = CrossSourceResult(
            public_records=len(public_data),
            warehouse_records=len(warehouse_data),
            joined_records=len(joined),
            join_key=join_key,
            records=joined,
        )

        logger.info(
            "Cross-source join: %d public + %d warehouse → %d joined (key=%s)",
            len(public_data), len(warehouse_data), len(joined), join_key,
        )
        return result

    def join_large(
        self,
        public_data: list[dict[str, Any]],
        warehouse_data: list[dict[str, Any]],
        join_key: str,
        public_key: str | None = None,
        warehouse_key: str | None = None,
        batch_size: int = 10000,
    ) -> CrossSourceResult:
        """Join large datasets in batches to avoid memory issues.

        Processes warehouse data in chunks, building index incrementally.
        """
        pk = public_key or join_key
        wk = warehouse_key or join_key

        # Build public data index (smaller side typically)
        public_index: dict[str, dict[str, Any]] = {}
        for record in public_data:
            key = record.get(pk)
            if key is not None:
                public_index[str(key).lower()] = record

        # Process warehouse data in batches
        joined: list[dict[str, Any]] = []
        for i in range(0, len(warehouse_data), batch_size):
            batch = warehouse_data[i:i + batch_size]
            for w_record in batch:
                key = w_record.get(wk)
                if key is None:
                    continue

                pub_match = public_index.get(str(key).lower())
                if pub_match:
                    merged = {**pub_match}
                    for k, v in w_record.items():
                        if k not in merged:
                            merged[k] = v
                        else:
                            merged[f"warehouse_{k}"] = v
                    joined.append(merged)

        return CrossSourceResult(
            public_records=len(public_data),
            warehouse_records=len(warehouse_data),
            joined_records=len(joined),
            join_key=join_key,
            records=joined,
        )
