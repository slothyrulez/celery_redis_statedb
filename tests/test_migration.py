import shelve
import zlib
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from celery.utils.collections import LimitedSet
from kombu.serialization import pickle, pickle_protocol

from celery_redis_statedb.migration import StateDBMigrator

"""Tests for state database migration from file to Redis."""


class TestStateDBMigrator:
    """Test StateDBMigrator class."""

    @pytest.fixture
    def temp_statedb(self, tmp_path: Path) -> Path:
        """Create a temporary statedb file with test data."""
        db_path = tmp_path / "worker.db"

        # Create test data
        revoked_tasks = LimitedSet()
        revoked_tasks.add("task-id-1")
        revoked_tasks.add("task-id-2")
        revoked_tasks.add("task-id-3")

        # Write to shelve database (v3 format)
        with shelve.open(str(db_path), protocol=pickle_protocol) as db:
            db["__proto__"] = 3
            db["zrevoked"] = zlib.compress(pickle.dumps(revoked_tasks, protocol=pickle_protocol))
            db["clock"] = 42

        return db_path

    @pytest.fixture
    def temp_statedb_v2(self, tmp_path: Path) -> Path:
        """Create a temporary statedb file with v2 format data."""
        db_path = tmp_path / "worker_v2.db"

        # Create test data (v2 format - direct LimitedSet)
        revoked_tasks = LimitedSet()
        revoked_tasks.add("task-id-4")
        revoked_tasks.add("task-id-5")

        with shelve.open(str(db_path), protocol=pickle_protocol) as db:
            db["__proto__"] = 2
            db["revoked"] = revoked_tasks
            db["clock"] = 10

        return db_path

    def test_migration_init_with_valid_path(self, temp_statedb: Path, redis_db) -> None:
        """Test initialization with valid statedb path."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        assert migration.statedb_filepath == temp_statedb
        assert migration.redis_state_db == redis_db

    def test_migration_init_with_invalid_path(self, tmp_path: Path, redis_db) -> None:
        """Test initialization with non-existent path."""
        invalid_path = tmp_path / "nonexistent.db"

        # Constructor doesn't raise, but run() will return False
        migration = StateDBMigrator(str(invalid_path), redis_db)
        assert migration.run() is False

    def test_backup_statedb_creates_backup(self, temp_statedb: Path, redis_db) -> None:
        """Test that backup file is created."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        backup_path = migration._backup_statedb()

        # Verify backup was created
        assert backup_path is not None
        assert backup_path.exists()
        assert backup_path.name.startswith("backup-")
        assert backup_path.parent == temp_statedb.parent

    def test_load_from_shelve_v3_format(self, temp_statedb: Path, redis_db) -> None:
        """Test loading state from v3 format shelve database."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        state_data = migration._load_from_shelve()

        assert "__proto__" in state_data
        assert state_data["__proto__"] == 3
        assert "zrevoked" in state_data
        assert "clock" in state_data
        assert state_data["clock"] == 42

    def test_load_from_shelve_v2_format(self, temp_statedb_v2: Path, redis_db) -> None:
        """Test loading state from v2 format shelve database."""
        migration = StateDBMigrator(str(temp_statedb_v2), redis_db)

        state_data = migration._load_from_shelve()

        assert "__proto__" in state_data
        assert state_data["__proto__"] == 2
        assert "revoked" in state_data
        assert isinstance(state_data["revoked"], LimitedSet)
        assert state_data["clock"] == 10

    def test_migrate_to_redis_v3_format(self, temp_statedb: Path, redis_db) -> None:
        """Test migration from v3 format to Redis."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        success = migration._migrate_to_redis()

        assert success is True

        # Verify data was written to Redis
        zrevoked = redis_db.get_zrevoked()
        assert zrevoked is not None
        assert isinstance(zrevoked, LimitedSet)
        assert "task-id-1" in zrevoked
        assert "task-id-2" in zrevoked
        assert "task-id-3" in zrevoked

        # Check clock value
        clock_value = redis_db.get_clock()
        assert clock_value == 42

    def test_migrate_to_redis_v2_format(self, temp_statedb_v2: Path, redis_db) -> None:
        """Test migration from v2 format to Redis."""
        migration = StateDBMigrator(str(temp_statedb_v2), redis_db)

        success = migration._migrate_to_redis()

        assert success is True

        # Verify data was written to Redis
        zrevoked = redis_db.get_zrevoked()
        assert zrevoked is not None
        assert isinstance(zrevoked, LimitedSet)
        assert "task-id-4" in zrevoked
        assert "task-id-5" in zrevoked

        clock_value = redis_db.get_clock()
        assert clock_value == 10

    def test_migrate_to_redis_empty_state(self, tmp_path: Path, redis_db) -> None:
        """Test migration with empty/no revoked tasks."""
        db_path = tmp_path / "empty.db"

        # Create empty database
        with shelve.open(str(db_path), protocol=pickle_protocol) as db:
            db["clock"] = 5

        migration = StateDBMigrator(str(db_path), redis_db)
        success = migration._migrate_to_redis()

        assert success is True
        # Should still set clock value
        clock_value = redis_db.get_clock()
        assert clock_value == 5

    def test_migrate_to_redis_handles_update_failure(self, temp_statedb: Path, redis_db) -> None:
        """Test handling of Redis update failure."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        # Simulate Redis connection error
        with patch.object(redis_db, "update", return_value=False):
            success = migration._migrate_to_redis()
            assert success is False

    def test_run_full_migration(self, temp_statedb: Path, redis_db) -> None:
        """Test full migration process."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        success = migration.run()

        assert success is True
        # Verify backup was created
        backup_files = list(temp_statedb.parent.glob("backup-*"))
        assert len(backup_files) > 0
        # Verify original file was renamed
        migrated_files = list(temp_statedb.parent.glob("*.migrated.*"))
        assert len(migrated_files) > 0
        # Verify original file no longer exists at original path
        assert not temp_statedb.exists()

    def test_rename_original_db(self, temp_statedb: Path, redis_db) -> None:
        """Test that original file is renamed after migration."""
        migration = StateDBMigrator(str(temp_statedb), redis_db)

        success, renamed_path = migration._rename_original_db()

        assert success is True
        assert renamed_path is not None
        assert ".migrated." in renamed_path.name
        assert renamed_path.exists()
        assert not temp_statedb.exists()  # Original should be gone

    def test_migration_with_corrupted_data(self, tmp_path: Path, redis_db) -> None:
        """Test handling of corrupted statedb data."""
        db_path = tmp_path / "corrupted.db"

        # Create database with corrupted data
        with shelve.open(str(db_path), protocol=pickle_protocol) as db:
            db["zrevoked"] = b"corrupted_data_not_valid_pickle"
            db["clock"] = 10

        migration = StateDBMigrator(str(db_path), redis_db)

        # Should handle the error gracefully
        success = migration._migrate_to_redis()
        assert success is False


class TestMigrationIntegrationWithBootstep:
    """Test migration integration with RedisStatePersistence bootstep."""

    def test_bootstep_accepts_migrate_statedb_parameter(self) -> None:
        """Test that RedisStatePersistence accepts migrate_statedb parameter."""
        from celery_redis_statedb.bootstep import RedisStatePersistence

        mock_worker = Mock()
        mock_worker.hostname = "test-worker"

        # Should not raise an error
        bootstep = RedisStatePersistence(
            mock_worker,
            redis_statedb="redis://localhost:6379/0",
            migrate_statedb="/path/to/worker.db",
        )

        assert bootstep.migrate_statedb == "/path/to/worker.db"
        assert bootstep.redis_statedb == "redis://localhost:6379/0"
