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
    with patch("spdatalab.common.file_utils.open_file", mock_open(read_data="".join(lines))):
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
    with patch("spdatalab.common.file_utils.open_file", side_effect=open_file_side_effects):
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
    with patch("spdatalab.common.file_utils.open_file", side_effect=open_file_side_effects):
        result = gen.generate_scene_list("index.txt", str(output_file))
    # 检查输出文件内容
    import json
    with open(output_file, "r") as f:
        data = json.load(f)
    assert data == [{"a": 1}, {"a": 1}]
    assert result == [{"a": 1}, {"a": 1}] 