import datetime
import logging
import shelve
import shutil
from pathlib import Path
from typing import Any

from celery.utils.collections import LimitedSet  # type: ignore[attr-defined]
from celery.worker.state import Persistent
from kombu.serialization import pickle  # type: ignore[attr-defined]

from celery_redis_statedb.state import RedisStateDB

logger = logging.getLogger(__name__)


class StateDBMigrator:
    """Handles migration from file-based statedb to Redis.

    This class reads data from Celery's shelve-based state persistence
    and stores it on Redis. The migration process:

    1. Creates a timestamped backup of the original file
    2. Migrates data (revoked tasks and clock) to Redis
    3. Renames the original file with .migrated.TIMESTAMP suffix

    The rename prevents accidental reuse of stale data while keeping the file
    as an additional backup (providing double redundancy with the backup file).

    Note:
        This implementation assumes the shelve database consists of a single file.
        While Python's shelve module may use different dbm backends (gdbm, ndbm, dumbdbm)
        that can create multiple files (.dir, .dat, .bak), this code is designed for
        environments using gdbm (GNU dbm) which stores data in a single .db file.
        This is the default on most modern Linux systems.
    """

    protocol = Persistent.protocol
    decompress = Persistent.decompress

    def __init__(
        self,
        statedb_path: str,
        redis_state_db: RedisStateDB,
    ) -> None:
        """Initialize migration handler.

        Args:
            statedb_path: Path to the file-based statedb
            redis_db: RedisStateDB instance to migrate data into
        """
        self.statedb_filepath = Path(statedb_path)
        self.redis_state_db = redis_state_db
        logger.info("[redis-statedb] Initializing migration from %s", statedb_path)

    def _get_timestamp(self) -> str:
        return datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _test_statedb_file(self) -> bool:
        _exists = self.statedb_filepath.exists()
        if _exists is False:
            logger.error("[redis-statedb] State database file not found: %s", self.statedb_filepath)
        return _exists

    def _backup_statedb(self) -> Path | None:
        """Create a backup of the statedb file.

        The backup is created in the same directory with a timestamp suffix.

        Returns:
            Path to the backup file if operation succeeded else None
        """
        timestamp = self._get_timestamp()
        backup_filepath = (
            self.statedb_filepath.parent
            / f"backup-{self.statedb_filepath.stem}.{timestamp}{self.statedb_filepath.suffix}"
        )
        logger.info("[redis-statedb] Creating backup: %s", backup_filepath)

        try:
            shutil.copy2(self.statedb_filepath, backup_filepath)
            logger.debug(
                "[redis-statedb] Backed up %s to %s",
                self.statedb_filepath,
                backup_filepath,
            )
            logger.info("[redis-statedb] Backup created successfully")
            return backup_filepath

        except OSError as exc:
            logger.error("[redis-statedb] Failed to create backup: %s", exc)
            return None

    def _load_from_shelve(self) -> dict[str, Any]:
        """Load state data from shelve database.

        Returns:
            Dictionary containing the state data (zrevoked, clock, __proto__)

        Raises:
            Exception: If reading from shelve fails
        """
        logger.info("[redis-statedb] Reading state from %s", self.statedb_filepath)

        try:
            # Open shelve database in read-only mode
            db = shelve.open(str(self.statedb_filepath), flag="r", protocol=self.protocol)

            try:
                state_data = dict(db)
                logger.info(
                    "[redis-statedb] Loaded state with keys: %s",
                    list(state_data.keys()),
                )
                return state_data
            finally:
                db.close()

        except Exception as exc:
            logger.error("[redis-statedb] Failed to read from statedb: %s", exc)
            raise

    def _migrate_to_redis(self) -> bool:
        """Migrate state data from file to Redis.

        This reads the shelve database, extracts revoked tasks and clock value,
        and stores them in Redis using the RedisStateDB API.

        Returns:
            True if migration was successful, False otherwise
        """
        logger.info("[redis-statedb] Migrating state to redis")

        try:
            state_data = self._load_from_shelve()

            zrevoked = None
            if "zrevoked" in state_data:
                # Version 3 format (compressed and pickled)
                zrevoked_data = state_data["zrevoked"]
                zrevoked = pickle.loads(self.decompress(zrevoked_data))
                logger.info("[redis-statedb] Loaded revoked v3 tasks")
            elif "revoked" in state_data:
                # Version 2 format (direct LimitedSet)
                zrevoked = state_data["revoked"]
                if isinstance(zrevoked, LimitedSet):
                    logger.info("[redis-statedb] Loaded v2 revoked tasks")
                else:
                    # Version 1 format (dict/set)
                    zrevoked_set = LimitedSet()
                    for item in state_data["revoked"]:
                        zrevoked_set.add(item)
                    zrevoked = zrevoked_set
                    logger.info("[redis-statedb] Loaded v1 revoked tasks")

            # Extract clock value
            clock_value = state_data.get("clock", None)

            if zrevoked is not None:
                logger.info(
                    "[redis-statedb] Migrating %d revoked tasks to Redis",
                    len(zrevoked),
                )
                # Use clock_value or 0 if not present (for the update call)
                success = self.redis_state_db.update(zrevoked=zrevoked, clock=clock_value)

                if success:
                    logger.info("[redis-statedb] Migration completed successfully")
                else:
                    logger.error("[redis-statedb] Failed to update Redis with migrated data")

                return success
            else:
                logger.info("[redis-statedb] No revoked tasks found in statedb")
                # Only set clock if it was explicitly stored in the old statedb
                if clock_value is not None:
                    self.redis_state_db.set_clock(clock_value)
                    logger.debug("[redis-statedb] Clock value %d saved to Redis", clock_value)
                else:
                    logger.info(
                        "[redis-statedb] No clock value in old statedb, "
                        "skipping clock migration (will be initialized by worker)"
                    )
                return True

        except Exception as exc:
            logger.error("[redis-statedb] Migration failed: %s", exc)
            return False

    def _rename_original_db(self) -> tuple[bool, Path | None]:
        """Rename the original statedb file to mark it as migrated.

        The file is renamed with a .migrated.TIMESTAMP suffix to prevent
        accidental reuse while keeping it as additional backup.

        Returns:
            Tuple of (success, renamed_path). renamed_path is None if operation failed.
        """
        timestamp = self._get_timestamp()
        renamed_filepath = (
            self.statedb_filepath.parent / f"{self.statedb_filepath.name}.migrated.{timestamp}"
        )

        logger.info("[redis-statedb] Renaming original db to prevent accidental reuse")
        try:
            self.statedb_filepath.rename(renamed_filepath)
            logger.info(
                "[redis-statedb] Original file renamed: %s -> %s",
                self.statedb_filepath.name,
                renamed_filepath.name,
            )
            return True, renamed_filepath
        except (FileNotFoundError, OSError) as exc:
            logger.error("[redis-statedb] Original db rename failed: %s", exc)
            return False, None

    def run(self) -> bool:
        """Execute the full migration process.

        Steps:
            1. Verify the statedb file exists
            2. Create a backup with timestamp
            3. Migrate data to Redis (revoked tasks + clock)
            4. Rename original file to .migrated.TIMESTAMP

        Returns:
            True if all steps completed successfully. Returns False if any critical
            step fails, including the rename operation (which is required to prevent
            accidental reuse of stale data on worker restart).
        """
        try:
            # Perform check
            test_db_file = self._test_statedb_file()
            if test_db_file:
                # Perform backup
                backup_filepath = self._backup_statedb()
                if backup_filepath is not None:
                    self.backup_filepath = backup_filepath
                    # Perform migration
                    migrated_to_redis = self._migrate_to_redis()
                    if migrated_to_redis:
                        # Rename original file to prevent accidental reuse
                        renamed_success, renamed_path = self._rename_original_db()
                        if renamed_success:
                            logger.info(
                                "[redis-statedb] Migration completed successfully. "
                                "Backup: %s, Renamed original: %s",
                                backup_filepath,
                                renamed_path,
                            )
                            return True
                        else:
                            logger.error(
                                "[redis-statedb] CRITICAL: Migration to Redis succeeded but original file "
                                "could not be renamed. This is dangerous - the stale file at %s could "
                                "overwrite Redis data on restart. Manual intervention required. "
                                "Backup is available at: %s",
                                self.statedb_filepath,
                                backup_filepath,
                            )
                            return False
        except Exception as exc:
            logger.error("[redis-statedb] Migration process failed: %s", exc)

        return False
