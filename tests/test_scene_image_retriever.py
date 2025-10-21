"""
场景图片检索器单元测试

测试SceneImageRetriever和SceneImageHTMLViewer的核心功能。

注意：由于依赖OBS和数据库，完整测试需要在远端环境运行。
"""

import pytest
import io
from pathlib import Path
from PIL import Image
import pandas as pd

from spdatalab.dataset.scene_image_retriever import (
    ImageFrame,
    SceneImageRetriever
)
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer


class TestImageFrame:
    """测试ImageFrame数据结构"""
    
    def test_image_frame_creation(self):
        """测试ImageFrame创建"""
        # 创建一个简单的测试图片
        img = Image.new('RGB', (100, 100), color='red')
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        image_data = buffer.getvalue()
        
        frame = ImageFrame(
            scene_id="test_scene_001",
            frame_index=0,
            timestamp=1234567890000,
            image_data=image_data,
            image_format='png'
        )
        
        assert frame.scene_id == "test_scene_001"
        assert frame.frame_index == 0
        assert frame.timestamp == 1234567890000
        assert frame.image_format == 'png'
        assert len(frame.image_data) > 0
    
    def test_to_pil_image(self):
        """测试转换为PIL Image"""
        # 创建测试图片
        img = Image.new('RGB', (100, 100), color='blue')
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG')
        image_data = buffer.getvalue()
        
        frame = ImageFrame(
            scene_id="test_scene_002",
            frame_index=1,
            timestamp=1234567890000,
            image_data=image_data,
            image_format='jpeg'
        )
        
        # 转换为PIL Image
        pil_img = frame.to_pil_image()
        assert isinstance(pil_img, Image.Image)
        assert pil_img.size == (100, 100)


class TestSceneImageRetriever:
    """测试SceneImageRetriever类"""
    
    def test_initialization(self):
        """测试初始化"""
        retriever = SceneImageRetriever()
        assert retriever.camera_type == "CAM_FRONT_WIDE_ANGLE"
        
        retriever_custom = SceneImageRetriever(camera_type="CAM_FRONT")
        assert retriever_custom.camera_type == "CAM_FRONT"
    
    def test_detect_image_format_png(self):
        """测试PNG格式检测"""
        retriever = SceneImageRetriever()
        
        # 创建PNG图片
        img = Image.new('RGB', (50, 50), color='green')
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        png_data = buffer.getvalue()
        
        format_detected = retriever._detect_image_format(png_data)
        assert format_detected == 'png'
    
    def test_detect_image_format_jpeg(self):
        """测试JPEG格式检测"""
        retriever = SceneImageRetriever()
        
        # 创建JPEG图片
        img = Image.new('RGB', (50, 50), color='yellow')
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG')
        jpeg_data = buffer.getvalue()
        
        format_detected = retriever._detect_image_format(jpeg_data)
        assert format_detected == 'jpeg'
    
    def test_parse_camera_parquet_path(self):
        """测试相机parquet路径解析"""
        retriever = SceneImageRetriever(camera_type="CAM_FRONT_WIDE_ANGLE")
        
        # 测试不同格式的输入
        path1 = "obs://bucket/scene_001"
        result1 = retriever._parse_camera_parquet_path(path1)
        assert result1 == "obs://bucket/scene_001/samples/CAM_FRONT_WIDE_ANGLE"
        
        path2 = "bucket/scene_002/"
        result2 = retriever._parse_camera_parquet_path(path2)
        assert result2 == "obs://bucket/scene_002/samples/CAM_FRONT_WIDE_ANGLE"
    
    def test_parse_parquet_to_frames(self):
        """测试parquet数据解析为帧"""
        retriever = SceneImageRetriever()
        
        # 创建模拟的parquet数据
        img1 = Image.new('RGB', (100, 100), color='red')
        buffer1 = io.BytesIO()
        img1.save(buffer1, format='PNG')
        
        img2 = Image.new('RGB', (100, 100), color='blue')
        buffer2 = io.BytesIO()
        img2.save(buffer2, format='JPEG')
        
        # 创建DataFrame
        df = pd.DataFrame({
            'image': [buffer1.getvalue(), buffer2.getvalue()],
            'timestamp': [1000000, 2000000],
            'filename': ['frame_000.png', 'frame_001.jpg']
        })
        
        frames = retriever._parse_parquet_to_frames(
            df, 
            scene_id="test_scene",
            max_frames=2
        )
        
        assert len(frames) == 2
        assert frames[0].scene_id == "test_scene"
        assert frames[0].frame_index == 0
        assert frames[1].frame_index == 1
        assert frames[0].image_format == 'png'
        assert frames[1].image_format == 'jpeg'
    
    def test_parse_parquet_with_frame_indices(self):
        """测试按索引过滤帧"""
        retriever = SceneImageRetriever()
        
        # 创建5帧数据
        frames_data = []
        for i in range(5):
            img = Image.new('RGB', (50, 50), color='red')
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            frames_data.append(buffer.getvalue())
        
        df = pd.DataFrame({
            'image': frames_data,
            'timestamp': list(range(5))
        })
        
        # 只提取索引0, 2, 4的帧
        frames = retriever._parse_parquet_to_frames(
            df,
            scene_id="test_scene",
            frame_indices=[0, 2, 4]
        )
        
        assert len(frames) == 3
        assert frames[0].frame_index == 0
        assert frames[1].frame_index == 2
        assert frames[2].frame_index == 4


class TestSceneImageHTMLViewer:
    """测试SceneImageHTMLViewer类"""
    
    def test_initialization(self):
        """测试初始化"""
        viewer = SceneImageHTMLViewer()
        assert viewer is not None
    
    def test_encode_image_base64(self):
        """测试base64编码"""
        viewer = SceneImageHTMLViewer()
        
        # 创建测试图片
        img = Image.new('RGB', (50, 50), color='red')
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        image_data = buffer.getvalue()
        
        base64_str = viewer._encode_image_base64(image_data, 'png')
        
        assert base64_str.startswith('data:image/png;base64,')
        assert len(base64_str) > 100  # 应该有一定长度
    
    def test_create_thumbnail(self):
        """测试缩略图创建"""
        viewer = SceneImageHTMLViewer()
        
        # 创建一个大图片
        img = Image.new('RGB', (800, 600), color='blue')
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG')
        image_data = buffer.getvalue()
        
        # 创建缩略图
        thumbnail_data = viewer._create_thumbnail(image_data, max_size=200)
        
        # 验证缩略图
        thumbnail_img = Image.open(io.BytesIO(thumbnail_data))
        assert max(thumbnail_img.size) <= 200
        assert thumbnail_img.size[0] <= 200
        assert thumbnail_img.size[1] <= 200
    
    def test_format_timestamp(self):
        """测试时间戳格式化"""
        viewer = SceneImageHTMLViewer()
        
        # 测试毫秒时间戳
        ts_ms = 1697875200000  # 2023-10-21 08:00:00
        formatted = viewer._format_timestamp(ts_ms)
        assert '2023' in formatted
        assert '10' in formatted
        assert '21' in formatted
        
        # 测试微秒时间戳
        ts_us = 1697875200000000
        formatted_us = viewer._format_timestamp(ts_us)
        assert '2023' in formatted_us
    
    def test_generate_html_report(self, tmp_path):
        """测试HTML报告生成"""
        viewer = SceneImageHTMLViewer()
        
        # 创建测试数据
        img1 = Image.new('RGB', (100, 100), color='red')
        buffer1 = io.BytesIO()
        img1.save(buffer1, format='PNG')
        
        img2 = Image.new('RGB', (100, 100), color='blue')
        buffer2 = io.BytesIO()
        img2.save(buffer2, format='JPEG')
        
        frame1 = ImageFrame(
            scene_id="scene_001",
            frame_index=0,
            timestamp=1000000,
            image_data=buffer1.getvalue(),
            image_format='png'
        )
        
        frame2 = ImageFrame(
            scene_id="scene_001",
            frame_index=1,
            timestamp=2000000,
            image_data=buffer2.getvalue(),
            image_format='jpeg'
        )
        
        images_dict = {
            "scene_001": [frame1, frame2]
        }
        
        # 生成HTML
        output_path = tmp_path / "test_report.html"
        result_path = viewer.generate_html_report(
            images_dict,
            str(output_path),
            title="测试报告"
        )
        
        # 验证文件存在
        assert Path(result_path).exists()
        
        # 读取并验证HTML内容
        with open(result_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        assert "测试报告" in html_content
        assert "scene_001" in html_content
        assert "data:image/png;base64," in html_content
        assert "data:image/jpeg;base64," in html_content
        assert "总场景数: 1" in html_content
        assert "总帧数: 2" in html_content


# 集成测试标记（需要数据库和OBS访问）
@pytest.mark.integration
class TestIntegration:
    """集成测试（需要真实环境）"""
    
    def test_full_workflow(self):
        """测试完整工作流
        
        注意：此测试需要：
        1. 有效的数据库连接
        2. OBS访问权限
        3. 有效的scene_id
        
        在远端环境运行：
        pytest tests/test_scene_image_retriever.py::TestIntegration -v
        """
        # 使用一个已知存在的scene_id（需要根据实际情况修改）
        test_scene_id = "your_valid_scene_id"
        
        # 1. 加载图片
        retriever = SceneImageRetriever()
        frames = retriever.load_images_from_scene(test_scene_id, max_frames=2)
        
        assert len(frames) > 0
        assert frames[0].scene_id == test_scene_id
        
        # 2. 生成HTML
        viewer = SceneImageHTMLViewer()
        images_dict = {test_scene_id: frames}
        
        report_path = viewer.generate_html_report(
            images_dict,
            "test_integration_report.html",
            title="集成测试报告"
        )
        
        assert Path(report_path).exists()
        
        # 清理
        Path(report_path).unlink()


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])

