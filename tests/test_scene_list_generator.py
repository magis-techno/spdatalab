"""场景数据列表生成器的单元测试。"""

import pytest
from unittest.mock import patch, mock_open
from spdatalab.dataset.scene_list_generator import SceneListGenerator
from spdatalab.common.decoder import decode_shrink_line as decoder_decode_shrink_line

def test_parse_index_line_valid():
    gen = SceneListGenerator()
    assert gen.parse_index_line("obs://bucket/file.shrink@duplicate10") == ("obs://bucket/file.shrink", 10)
    assert gen.parse_index_line("localfile.shrink@duplicate2") == ("localfile.shrink", 2)

def test_parse_index_line_invalid():
    gen = SceneListGenerator()
    assert gen.parse_index_line("") is None
    assert gen.parse_index_line("invalid_line") is None
    assert gen.parse_index_line("file@dup") is None

def test_decode_shrink_line_json():
    d = {"a": 1}
    line = '{"a": 1}'
    assert decoder_decode_shrink_line(line) == d

def test_decode_shrink_line_compressed():
    import base64, gzip, json
    d = {"b": 2}
    raw = json.dumps(d).encode("utf-8")
    compressed = gzip.compress(raw)
    encoded = base64.b64encode(compressed).decode("ascii")
    assert decoder_decode_shrink_line(encoded) == d

def test_decode_shrink_line_invalid():
    assert decoder_decode_shrink_line("") is None
    assert decoder_decode_shrink_line("notbase64") is None

def test_iter_scenes_from_file_and_stats():
    # 模拟文件内容
    lines = ['{"a": 1}\n', 'notjson\n', '{"b": 2}\n']
    gen = SceneListGenerator()
    with patch("spdatalab.dataset.scene_list_generator.open_file", mock_open(read_data="".join(lines))):
        scenes = list(gen.iter_scenes_from_file("dummy.txt"))
    assert scenes == [{"a": 1}, {"b": 2}]
    assert gen.stats["failed_scenes"] == 1

def test_iter_scene_list_and_duplication():
    # 索引文件内容
    index_content = "file1@duplicate2\nfile2@duplicate1\n"
    # 文件内容
    file1_lines = ['{"x": 1}\n']
    file2_lines = ['{"y": 2}\n', '{"z": 3}\n']
    gen = SceneListGenerator()
    # patch open_file: 第一次打开是索引，后面依次是file1, file2
    m = mock_open(read_data=index_content)
    m2 = mock_open(read_data="".join(file1_lines))
    m3 = mock_open(read_data="".join(file2_lines))
    open_file_side_effects = [m.return_value, m2.return_value, m3.return_value]
    with patch("spdatalab.dataset.scene_list_generator.open_file", side_effect=open_file_side_effects):
        scenes = list(gen.iter_scene_list("index.txt"))
    # file1 1行*2倍增，file2 2行*1倍增
    assert scenes == [{"x": 1}, {"x": 1}, {"y": 2}, {"z": 3}]
    assert gen.stats["total_files"] == 2
    assert gen.stats["total_scenes"] == 4

def test_generate_scene_list_output(tmp_path):
    # 索引和数据内容
    index_content = "file1@duplicate2\n"
    file1_lines = ['{"a": 1}\n']
    gen = SceneListGenerator()
    m = mock_open(read_data=index_content)
    m2 = mock_open(read_data="".join(file1_lines))
    open_file_side_effects = [m.return_value, m2.return_value]
    output_file = tmp_path / "out.json"
    with patch("spdatalab.dataset.scene_list_generator.open_file", side_effect=open_file_side_effects):
        result = gen.generate_scene_list("index.txt", str(output_file))
    # 检查输出文件内容
    import json
    with open(output_file, "r") as f:
        data = json.load(f)
    assert data == [{"a": 1}, {"a": 1}]
    assert result == [{"a": 1}, {"a": 1}]

def test_iter_scenes_from_obs_file():
    """
    集成测试：实际读取OBS上的shrink文件，验证能否正常读取内容。
    需要OBS环境和有效凭证，默认跳过。
    取消skip装饰器后可手动运行。
    """
    pytest.skip("需要OBS环境和凭证，手动测试时可去掉本行")
    from spdatalab.dataset.scene_list_generator import SceneListGenerator
    obs_paths = [
        "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink",
        "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_stuck_veh_sub_ddi_2773412e2e_2025_05_18_10_37_48/train_god_god_E2E_0419_7_6_0_9_20250518104817_839_21601_duplicate_26431_guiyang_f_pkg2_2frames.jsonl.shrink",
        "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_vru_avoid_obstacle_data_sub_ddi_2773412e2e_2025_05_18_10_10_41/train_god_god_E2E_0419_7_6_0_9_20250518104658_6844_218135_duplicate_441684_guiyang_f_pkg2_2frames.jsonl.shrink"
    ]
    gen = SceneListGenerator()
    for path in obs_paths:
        scenes = list(gen.iter_scenes_from_file(path))
        print(f"{path} 读取到 {len(scenes)} 条数据，失败行数：{gen.stats['failed_scenes']}")
        print("前3条：", scenes[:3])

def test_iter_shrink_lines_from_obs_file():
    """
    集成测试：逐行读取OBS上的jsonl.shrink文件，并用decode_shrink_line解码。
    需要OBS环境和有效凭证，默认跳过。
    取消skip装饰器后可手动运行。
    """
    import pytest
    pytest.skip("需要OBS环境和凭证，手动测试时可去掉本行")
    from spdatalab.common.decoder import decode_shrink_line
    from spdatalab.common.file_utils import open_file
    obs_path = "obs://yw-ads-training-gy1/data/god/autoscenes-prod/index/god/GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59/train_god_god_E2E_0419_7_6_0_9_20250518112117_1323_32968_duplicate_61858_guiyang_f_pkg2_2frames.jsonl.shrink"
    count = 0
    failed = 0
    with open_file(obs_path, 'r') as f:
        for i, line in enumerate(f):
            obj = decode_shrink_line(line)
            if obj is not None:
                if i < 3:
                    print(obj)
                count += 1
            else:
                failed += 1
    print(f"共读取到 {count} 条有效数据，失败 {failed} 条") 