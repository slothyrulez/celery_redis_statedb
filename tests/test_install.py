"""Unit tests for install_redis_statedb convenience function."""

from unittest.mock import Mock, patch

import pytest

from celery_redis_statedb import install_redis_statedb
from celery_redis_statedb.bootstep import RedisStatePersistence


class TestInstallRedisStateDB:
    """Test install_redis_statedb function."""

    def test_install_success(self) -> None:
        """Test successful installation of Redis StateDB."""
        # Create mock app
        app = Mock()
        app.main = "testapp"
        app.steps = {"worker": set()}

        # Mock the default StateDB
        with patch("celery.worker.components.StateDB") as mock_default_statedb:
            # Add default StateDB to app
            app.steps["worker"].add(mock_default_statedb)

            # Install Redis StateDB
            install_redis_statedb(app)

            # Verify default StateDB was removed
            assert mock_default_statedb not in app.steps["worker"]

            # Verify Redis StateDB was added
            assert RedisStatePersistence in app.steps["worker"]

    def test_install_without_default_statedb(self) -> None:
        """Test installation when default StateDB is not present."""
        # Create mock app without default StateDB
        app = Mock()
        app.main = "testapp"
        app.steps = {"worker": set()}

        # Install Redis StateDB
        install_redis_statedb(app)

        # Verify Redis StateDB was added
        assert RedisStatePersistence in app.steps["worker"]

    def test_install_idempotent(self) -> None:
        """Test that installing multiple times is safe."""
        app = Mock()
        app.main = "testapp"
        app.steps = {"worker": set()}

        # Install twice
        install_redis_statedb(app)
        install_redis_statedb(app)

        # Should only have one instance
        assert RedisStatePersistence in app.steps["worker"]
        # Sets automatically handle duplicates, so this is safe

    def test_install_with_app_name(self) -> None:
        """Test installation logs app name."""
        app = Mock()
        app.main = "myapp"
        app.steps = {"worker": set()}

        with patch("celery_redis_statedb.logger") as mock_logger:
            install_redis_statedb(app)

            # Verify info log was called with app name
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0]
            assert "Redis StateDB installed successfully" in call_args[0]
            assert call_args[1] == "myapp"

    def test_install_without_app_name(self) -> None:
        """Test installation when app.main is None."""
        app = Mock()
        app.main = None
        app.steps = {"worker": set()}

        with patch("celery_redis_statedb.logger") as mock_logger:
            install_redis_statedb(app)

            # Verify info log was called with default
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0]
            assert "Redis StateDB installed successfully" in call_args[0]
            assert call_args[1] == "app"

    def test_install_error_handling(self) -> None:
        """Test handling of errors during installation."""
        app = Mock()
        mock_worker_steps = Mock()
        mock_worker_steps.add = Mock(side_effect=Exception("Add failed"))
        mock_worker_steps.__contains__ = Mock(return_value=False)
        app.steps = {"worker": mock_worker_steps}

        with pytest.raises(Exception, match="Add failed"):
            install_redis_statedb(app)
