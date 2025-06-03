"""场景数据列表生成器的单元测试。"""

import pytest
import json
import base64
import gzip
from unittest.mock import patch, mock_open
from spdatalab.dataset.scene_list_generator import SceneListGenerator
from spdatalab.common.decoder import decode_shrink_line as decoder_decode_shrink_line

# 测试数据
TEST_DATA = {
    "valid_index_lines": [
        ("obs://bucket/file.shrink@duplicate10", ("obs://bucket/file.shrink", 10)),
        ("localfile.shrink@duplicate2", ("localfile.shrink", 2))
    ],
    "invalid_index_lines": ["", "invalid_line", "file@dup"],
    "json_data": {"a": 1},
    "compressed_data": {"b": 2}
}

# Fixtures
@pytest.fixture
def scene_list_generator():
    return SceneListGenerator()

@pytest.fixture
def mock_file_content():
    return {
        "index": "file1@duplicate2\nfile2@duplicate1\n",
        "file1": ['{"x": 1}\n'],
        "file2": ['{"y": 2}\n', '{"z": 3}\n']
    }

# 单元测试
@pytest.mark.unit
class TestSceneListGenerator:
    def test_parse_index_line_valid(self, scene_list_generator):
        for line, expected in TEST_DATA["valid_index_lines"]:
            assert scene_list_generator.parse_index_line(line) == expected

    def test_parse_index_line_invalid(self, scene_list_generator):
        for line in TEST_DATA["invalid_index_lines"]:
            assert scene_list_generator.parse_index_line(line) is None

    def test_decode_shrink_line_json(self):
        line = json.dumps(TEST_DATA["json_data"])
        assert decoder_decode_shrink_line(line) == TEST_DATA["json_data"]

    def test_decode_shrink_line_compressed(self):
        raw = json.dumps(TEST_DATA["compressed_data"]).encode("utf-8")
        compressed = gzip.compress(raw)
        encoded = base64.b64encode(compressed).decode("ascii")
        assert decoder_decode_shrink_line(encoded) == TEST_DATA["compressed_data"]

    def test_decode_shrink_line_invalid(self):
        assert decoder_decode_shrink_line("") is None
        assert decoder_decode_shrink_line("notbase64") is None

    def test_iter_scenes_from_file_and_stats(self, scene_list_generator):
        lines = ['{"a": 1}\n', 'notjson\n', '{"b": 2}\n']
        with patch("spdatalab.dataset.scene_list_generator.open_file", mock_open(read_data="".join(lines))):
            scenes = list(scene_list_generator.iter_scenes_from_file("dummy.txt"))
        assert scenes == [{"a": 1}, {"b": 2}]
        assert scene_list_generator.stats["failed_scenes"] == 1

    def test_iter_scene_list_and_duplication(self, scene_list_generator, mock_file_content):
        m = mock_open(read_data=mock_file_content["index"])
        m2 = mock_open(read_data="".join(mock_file_content["file1"]))
        m3 = mock_open(read_data="".join(mock_file_content["file2"]))
        open_file_side_effects = [m.return_value, m2.return_value, m3.return_value]
        
        with patch("spdatalab.dataset.scene_list_generator.open_file", side_effect=open_file_side_effects):
            scenes = list(scene_list_generator.iter_scene_list("index.txt"))
        
        assert scenes == [{"x": 1}, {"x": 1}, {"y": 2}, {"z": 3}]
        assert scene_list_generator.stats["total_files"] == 2
        assert scene_list_generator.stats["total_scenes"] == 4

    def test_generate_scene_list_output(self, scene_list_generator, tmp_path):
        index_content = "file1@duplicate2\n"
        file1_lines = ['{"a": 1}\n']
        m = mock_open(read_data=index_content)
        m2 = mock_open(read_data="".join(file1_lines))
        open_file_side_effects = [m.return_value, m2.return_value]
        
        output_file = tmp_path / "out.json"
        with patch("spdatalab.dataset.scene_list_generator.open_file", side_effect=open_file_side_effects):
            result = scene_list_generator.generate_scene_list("index.txt", str(output_file))
        
        with open(output_file, "r") as f:
            data = json.load(f)
        assert data == [{"a": 1}, {"a": 1}]
        assert result == [{"a": 1}, {"a": 1}]

# 集成测试
@pytest.mark.integration
class TestIntegration:
    @pytest.mark.skipif(not pytest.config.getoption("--run-obs-tests"), 
                       reason="需要 --run-obs-tests 参数来运行OBS测试")
    def test_iter_scenes_from_obs_file(self, scene_list_generator):
        """集成测试：实际读取OBS上的shrink文件，验证能否正常读取内容。"""
        obs_paths = [
            "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink",
            "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48/train_god_god_E2E_0419_7_6_0_9_20250518104817_839_21601_duplicate_26431_guiyang_f_pkg2_2frames.jsonl.shrink",
            "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_vru_avoid_obstacle_data_sub_ddi_2773412e2e_2025_05_18_10_10_41/train_god_god_E2E_0419_7_6_0_9_20250518104658_6844_218135_duplicate_441684_guiyang_f_pkg2_2frames.jsonl.shrink"
        ]
        for path in obs_paths:
            scenes = list(scene_list_generator.iter_scenes_from_file(path))
            assert len(scenes) > 0, f"{path} 没有读取到数据"
            assert scene_list_generator.stats["failed_scenes"] >= 0

    @pytest.mark.skipif(not pytest.config.getoption("--run-obs-tests"), 
                       reason="需要 --run-obs-tests 参数来运行OBS测试")
    def test_open_obs_file_env(self):
        """集成测试：验证 open_file 在打开 OBS 文件时是否正确初始化 moxing 环境。"""
        from spdatalab.common.file_utils import open_file
        obs_path = "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink"
        try:
            with open_file(obs_path, 'r') as f:
                scenes = list(f)
                assert len(scenes) > 0, "OBS 文件场景数为 0"
        except FileNotFoundError:
            pytest.skip("OBS 文件不存在，跳过测试")

    @pytest.mark.skipif(not pytest.config.getoption("--run-obs-tests"), 
                       reason="需要 --run-obs-tests 参数来运行OBS测试")
    def test_iter_shrink_lines_from_obs_file(self):
        """集成测试：逐行读取 OBS 上的 jsonl.shrink 文件，并用 decode_shrink_line 解码。"""
        from spdatalab.common.decoder import decode_shrink_line
        from spdatalab.common.file_utils import open_file
        obs_path = "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink"
        try:
            with open_file(obs_path, 'r') as f:
                count = 0
                failed = 0
                for i, line in enumerate(f):
                    obj = decode_shrink_line(line)
                    if obj is not None:
                        count += 1
                    else:
                        failed += 1
                assert count > 0, "OBS 文件场景数为 0"
                assert failed >= 0, "失败行数异常"
        except FileNotFoundError:
            pytest.skip("OBS 文件不存在，跳过测试")

    @pytest.mark.skipif(not pytest.config.getoption("--run-local-tests"), 
                       reason="需要 --run-local-tests 参数来运行本地文件测试")
    def test_local_shrink_file(self):
        """集成测试：验证本地 shrink 文件的读取，确保场景数大于 0。"""
        from spdatalab.common.file_utils import open_file
        local_path = "path/to/your/local/file.shrink"  # 替换为实际的本地文件路径
        try:
            with open_file(local_path, 'r') as f:
                scenes = list(f)
                assert len(scenes) > 0, "本地文件场景数为 0"
        except FileNotFoundError:
            pytest.skip("本地文件不存在，跳过测试")

# 添加命令行参数
def pytest_addoption(parser):
    parser.addoption("--run-obs-tests", action="store_true", help="运行OBS相关的集成测试")
    parser.addoption("--run-local-tests", action="store_true", help="运行本地文件相关的集成测试") 