import json
import sqlite3
from datetime import datetime
from typing import TypedDict, cast, final, override

from tl import JSONSerializable

from .storage import RunMetadata, StorageBackend, TestMetadata


class _SelectRow(TypedDict):
    run_id: str
    start_time: float
    end_time: float | None
    status: str
    metadata: str


@final
class SQLiteStorage(StorageBackend):
    def __init__(self, db_path: str | None = None):
        self.db_path: str = db_path or ":memory:"
        self.conn: sqlite3.Connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cursor = self.conn.cursor()

        _ = cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                start_time REAL NOT NULL,
                end_time REAL,
                status TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )

        self.conn.commit()

    @staticmethod
    def _row_to_run(row: _SelectRow) -> RunMetadata | None:
        metadata_json = cast(JSONSerializable, json.loads(row["metadata"]))
        if not isinstance(metadata_json, dict):
            return None
        metadata = TestMetadata.model_validate(metadata_json)

        return RunMetadata(
            run_id=row["run_id"],
            start_time=datetime.fromtimestamp(row["start_time"]),
            end_time=datetime.fromtimestamp(row["end_time"]) if row["end_time"] else None,
            status=row["status"],
            metadata=metadata,
        )

    @override
    async def register_run(self, run_id: str, metadata: TestMetadata) -> None:
        cursor = self.conn.cursor()
        _ = cursor.execute(
            """
            INSERT OR REPLACE INTO runs (run_id, start_time, end_time, status, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                datetime.now().timestamp(),
                None,
                "running",
                metadata.model_dump_json(),
            ),
        )
        self.conn.commit()

    @override
    async def update_run_status(
        self, run_id: str, status: str, end_time: datetime | None = None
    ) -> None:
        cursor = self.conn.cursor()
        _ = cursor.execute(
            """
            UPDATE runs SET status = ?, end_time = ? WHERE run_id = ?
            """,
            (
                status,
                end_time.timestamp() if end_time else datetime.now().timestamp(),
                run_id,
            ),
        )
        self.conn.commit()

    @override
    async def list_runs(self, limit: int = 50, offset: int = 0) -> list[RunMetadata]:
        cursor = self.conn.cursor()
        _ = cursor.execute(
            """
            SELECT run_id, start_time, end_time, status, metadata
            FROM runs
            ORDER BY start_time DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )

        runs: list[RunMetadata] = []
        for row in cast(list[_SelectRow], cursor.fetchall()):
            run = self._row_to_run(row)
            if run is not None:
                runs.append(run)
        return runs

    @override
    async def get_run_metadata(self, run_id: str) -> RunMetadata | None:
        cursor = self.conn.cursor()
        _ = cursor.execute(
            """
            SELECT run_id, start_time, end_time, status, metadata
            FROM runs
            WHERE run_id = ?
            """,
            (run_id,),
        )

        row = cast(_SelectRow | None, cursor.fetchone())
        if row is None:
            return None
        return self._row_to_run(row)

    @override
    async def list_active_runs(self) -> list[RunMetadata]:
        cursor = self.conn.cursor()
        _ = cursor.execute(
            """
            SELECT run_id, start_time, end_time, status, metadata
            FROM runs
            WHERE status = 'running'
            ORDER BY start_time DESC
            """
        )
        runs: list[RunMetadata] = []
        for row in cast(list[_SelectRow], cursor.fetchall()):
            run = self._row_to_run(row)
            if run is not None:
                runs.append(run)
        return runs

    def close(self) -> None:
        self.conn.close()
