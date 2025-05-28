from unittest import mock
from spdatalab.dataset.ingest import prepare_case
from pathlib import Path

@mock.patch('spdatalab.common.io_obs.download')
@mock.patch('spdatalab.common.io_hive.hive_cursor')
def test_prepare_case(mock_cursor_ctx, mock_download, tmp_path):
    mock_cursor = mock.MagicMock()
    mock_cursor.fetchone.return_value = (1, '/path', 'obs://bucket/key')
    mock_cursor_ctx.return_value.__enter__.return_value = mock_cursor
    assert prepare_case('demo', tmp_path)
    mock_download.assert_called_once()