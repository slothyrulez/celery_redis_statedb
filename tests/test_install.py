from unittest.mock import patch

import pytest
from celery import Celery
from click import Option

from celery_redis_statedb import install_redis_statedb
from celery_redis_statedb.bootstep import RedisStatePersistence

"""Unit tests for install_redis_statedb convenience function."""


class TestInstallRedisStateDB:
    """Test install_redis_statedb function."""

    def test_install_success(self, celery_app: Celery) -> None:
        """Test successful installation of Redis StateDB."""
        # Install Redis StateDB
        install_redis_statedb(celery_app)

        # Type narrowing for typechecker
        assert celery_app.steps is not None
        assert celery_app.user_options is not None

        # Verify Redis StateDB was added
        assert RedisStatePersistence in celery_app.steps["worker"]

        # Verify CLI options were added (--redis-statedb and --migrate-statedb)
        assert len(celery_app.user_options["worker"]) == 2
        options = list(celery_app.user_options["worker"])
        assert all(isinstance(opt, Option) for opt in options)
        option_names = {opt.opts[0] for opt in options}
        assert "--redis-statedb" in option_names
        assert "--migrate-statedb" in option_names

    def test_install_cli_option_parameters(self, celery_app: Celery) -> None:
        """Test that the CLI options have correct parameters."""
        install_redis_statedb(celery_app)

        # Type narrowing for typechecker
        assert celery_app.user_options is not None

        options = list(celery_app.user_options["worker"])

        # Find --redis-statedb option
        redis_opt = next(opt for opt in options if "--redis-statedb" in opt.opts)
        assert redis_opt.type.name == "text"  # str type in Click
        assert redis_opt.default is None
        assert "Redis URL for state persistence" in redis_opt.help

        # Find --migrate-statedb option
        migrate_opt = next(opt for opt in options if "--migrate-statedb" in opt.opts)
        assert migrate_opt.type.name == "text"  # str type in Click
        assert migrate_opt.default is None
        assert "Path to file-based statedb" in migrate_opt.help

    def test_install_idempotent(self, celery_app: Celery) -> None:
        """Test that installing multiple times is safe."""
        # Install twice
        install_redis_statedb(celery_app)
        install_redis_statedb(celery_app)

        # Type narrowing for typechecker
        assert celery_app.steps is not None

        # Should only have one instance
        assert RedisStatePersistence in celery_app.steps["worker"]
        # Sets automatically handle duplicates, so this is safe

    def test_install_with_app_name(self) -> None:
        """Test installation logs app name."""
        app = Celery("myapp")

        with patch("celery_redis_statedb.logger") as mock_logger:
            install_redis_statedb(app)

            # Verify info log was called with app name
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0]
            assert "[redis-statedb] Installed successfully" in call_args[0]
            assert call_args[1] == "myapp"

    def test_install_without_app_name(self) -> None:
        """Test installation when app.main is None."""
        app = Celery()
        # Celery apps created without a name have main set to the module name,
        # so we need to explicitly set it to None to test this edge case
        app.main = None

        with patch("celery_redis_statedb.logger") as mock_logger:
            install_redis_statedb(app)

            # Verify info log was called with default
            mock_logger.info.assert_called_once()
            call_args = mock_logger.info.call_args[0]
            assert "[redis-statedb] Installed successfully" in call_args[0]
            assert call_args[1] == "app"

    def test_install_error_handling(self, celery_app: Celery) -> None:
        """Test handling of errors during installation."""
        # Create a mock set that raises an error on add
        from unittest.mock import Mock

        mock_worker_steps = Mock()
        mock_worker_steps.add = Mock(side_effect=Exception("Add failed"))
        mock_worker_steps.__contains__ = Mock(return_value=False)
        celery_app.steps = {"worker": mock_worker_steps}

        with pytest.raises(Exception, match="Add failed"):
            install_redis_statedb(celery_app)
