"""
场景图片HTML查看器模块

生成独立的HTML报告用于快速浏览场景图片。

主要功能：
- 将图片嵌入为base64格式（单文件可移植）
- 响应式网格布局
- 场景分组显示
- 缩略图预览和全尺寸查看

作者：spdatalab
"""

import logging
import base64
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from io import BytesIO
from PIL import Image

from spdatalab.dataset.scene_image_retriever import ImageFrame

logger = logging.getLogger(__name__)


class SceneImageHTMLViewer:
    """场景图片HTML查看器
    
    生成包含base64编码图片的独立HTML文件，用于快速浏览。
    
    Example:
        >>> viewer = SceneImageHTMLViewer()
        >>> images = {"scene_001": [frame1, frame2], "scene_002": [frame3]}
        >>> report_path = viewer.generate_html_report(images, "report.html")
    """
    
    def __init__(self):
        logger.info("初始化SceneImageHTMLViewer")
    
    def _encode_image_base64(self, image_data: bytes, image_format: str) -> str:
        """将图片编码为base64字符串
        
        Args:
            image_data: 图片二进制数据
            image_format: 图片格式 ('png', 'jpeg')
            
        Returns:
            base64编码的data URI字符串
        """
        b64_data = base64.b64encode(image_data).decode('utf-8')
        
        # 确定MIME类型
        mime_type = f"image/{image_format}"
        if image_format == 'jpeg':
            mime_type = "image/jpeg"
        elif image_format == 'png':
            mime_type = "image/png"
        
        return f"data:{mime_type};base64,{b64_data}"
    
    def _create_thumbnail(self, image_data: bytes, max_size: int = 200) -> bytes:
        """创建缩略图
        
        Args:
            image_data: 原始图片数据
            max_size: 缩略图最大尺寸（像素）
            
        Returns:
            缩略图的二进制数据
        """
        try:
            img = Image.open(BytesIO(image_data))
            
            # 计算缩略图尺寸（保持宽高比）
            img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            # 保存为bytes
            output = BytesIO()
            img_format = img.format if img.format else 'JPEG'
            img.save(output, format=img_format, quality=85)
            
            return output.getvalue()
            
        except Exception as e:
            logger.warning(f"创建缩略图失败: {e}")
            # 返回原图
            return image_data
    
    def _format_timestamp(self, timestamp: int) -> str:
        """格式化时间戳
        
        Args:
            timestamp: 时间戳（毫秒或微秒）
            
        Returns:
            格式化的时间字符串
        """
        try:
            # 尝试毫秒时间戳
            if timestamp > 1e12:
                # 微秒
                dt = datetime.fromtimestamp(timestamp / 1e6)
            else:
                # 毫秒
                dt = datetime.fromtimestamp(timestamp / 1e3)
            
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except:
            return str(timestamp)
    
    def _generate_html_template(
        self,
        images_dict: Dict[str, List[ImageFrame]],
        title: str,
        thumbnail_size: int
    ) -> str:
        """生成HTML模板
        
        Args:
            images_dict: 场景ID到ImageFrame列表的映射
            title: 报告标题
            thumbnail_size: 缩略图大小
            
        Returns:
            完整的HTML字符串
        """
        # 统计信息
        total_scenes = len(images_dict)
        total_frames = sum(len(frames) for frames in images_dict.values())
        generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 生成场景HTML
        scenes_html = []
        
        for scene_id, frames in images_dict.items():
            if not frames:
                continue
            
            # 场景标题
            scene_html = f"""
            <div class="scene-group">
                <div class="scene-header" onclick="toggleScene('{scene_id}')">
                    <h2>场景: {scene_id}</h2>
                    <span class="frame-count">{len(frames)} 帧</span>
                    <span class="toggle-icon">▼</span>
                </div>
                <div class="scene-content" id="scene-{scene_id}">
                    <div class="image-grid">
            """
            
            # 图片卡片
            for frame in frames:
                # 创建缩略图
                thumbnail_data = self._create_thumbnail(frame.image_data, thumbnail_size)
                thumbnail_b64 = self._encode_image_base64(thumbnail_data, frame.image_format)
                
                # 全尺寸图片
                full_b64 = self._encode_image_base64(frame.image_data, frame.image_format)
                
                # 格式化时间戳
                timestamp_str = self._format_timestamp(frame.timestamp)
                
                # 图片卡片HTML
                card_html = f"""
                    <div class="image-card">
                        <div class="image-container">
                            <img src="{thumbnail_b64}" 
                                 alt="Frame {frame.frame_index}"
                                 class="thumbnail"
                                 onclick="showFullImage('{full_b64}', '{scene_id}', {frame.frame_index})">
                        </div>
                        <div class="image-meta">
                            <div class="meta-item">
                                <span class="meta-label">帧索引:</span>
                                <span class="meta-value">{frame.frame_index}</span>
                            </div>
                            <div class="meta-item">
                                <span class="meta-label">时间戳:</span>
                                <span class="meta-value">{timestamp_str}</span>
                            </div>
                            <div class="meta-item">
                                <span class="meta-label">格式:</span>
                                <span class="meta-value">{frame.image_format.upper()}</span>
                            </div>
                        </div>
                    </div>
                """
                
                scene_html += card_html
            
            scene_html += """
                    </div>
                </div>
            </div>
            """
            
            scenes_html.append(scene_html)
        
        # 完整HTML文档
        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f5f5f5;
            color: #333;
            line-height: 1.6;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 2rem;
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            font-size: 2rem;
            margin-bottom: 0.5rem;
        }}
        
        .stats {{
            display: flex;
            justify-content: center;
            gap: 2rem;
            margin-top: 1rem;
            font-size: 0.9rem;
        }}
        
        .stat-item {{
            background: rgba(255,255,255,0.2);
            padding: 0.5rem 1rem;
            border-radius: 4px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 1rem;
        }}
        
        .scene-group {{
            background: white;
            margin-bottom: 1.5rem;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .scene-header {{
            background: #f8f9fa;
            padding: 1rem 1.5rem;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 1rem;
            transition: background 0.2s;
        }}
        
        .scene-header:hover {{
            background: #e9ecef;
        }}
        
        .scene-header h2 {{
            flex: 1;
            font-size: 1.3rem;
            color: #495057;
        }}
        
        .frame-count {{
            background: #667eea;
            color: white;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.9rem;
        }}
        
        .toggle-icon {{
            font-size: 1.2rem;
            transition: transform 0.3s;
        }}
        
        .scene-header.collapsed .toggle-icon {{
            transform: rotate(-90deg);
        }}
        
        .scene-content {{
            padding: 1.5rem;
            transition: max-height 0.3s ease-out;
        }}
        
        .scene-content.hidden {{
            display: none;
        }}
        
        .image-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 1.5rem;
        }}
        
        .image-card {{
            background: #fff;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            overflow: hidden;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .image-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }}
        
        .image-container {{
            position: relative;
            width: 100%;
            padding-top: 75%; /* 4:3 aspect ratio */
            background: #f8f9fa;
            overflow: hidden;
        }}
        
        .thumbnail {{
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            object-fit: cover;
            cursor: pointer;
            transition: opacity 0.2s;
        }}
        
        .thumbnail:hover {{
            opacity: 0.8;
        }}
        
        .image-meta {{
            padding: 0.75rem;
            background: #f8f9fa;
        }}
        
        .meta-item {{
            display: flex;
            justify-content: space-between;
            font-size: 0.85rem;
            margin-bottom: 0.25rem;
        }}
        
        .meta-label {{
            color: #6c757d;
            font-weight: 500;
        }}
        
        .meta-value {{
            color: #495057;
            font-family: 'Courier New', monospace;
        }}
        
        /* 模态框样式 */
        .modal {{
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.9);
            z-index: 1000;
            cursor: pointer;
        }}
        
        .modal.active {{
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        
        .modal-content {{
            max-width: 90%;
            max-height: 90%;
            position: relative;
        }}
        
        .modal-image {{
            max-width: 100%;
            max-height: 90vh;
            object-fit: contain;
        }}
        
        .modal-info {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(0,0,0,0.7);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 4px;
            font-size: 0.9rem;
        }}
        
        .close-button {{
            position: absolute;
            top: 20px;
            right: 30px;
            color: white;
            font-size: 2rem;
            font-weight: bold;
            cursor: pointer;
            z-index: 1001;
        }}
        
        .close-button:hover {{
            color: #ccc;
        }}
        
        @media (max-width: 768px) {{
            .header h1 {{
                font-size: 1.5rem;
            }}
            
            .stats {{
                flex-direction: column;
                gap: 0.5rem;
            }}
            
            .image-grid {{
                grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
                gap: 1rem;
            }}
            
            .scene-header h2 {{
                font-size: 1.1rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <div class="stats">
            <div class="stat-item">总场景数: {total_scenes}</div>
            <div class="stat-item">总帧数: {total_frames}</div>
            <div class="stat-item">生成时间: {generation_time}</div>
        </div>
    </div>
    
    <div class="container">
        {''.join(scenes_html)}
    </div>
    
    <!-- 全屏图片模态框 -->
    <div id="imageModal" class="modal" onclick="closeModal()">
        <span class="close-button">&times;</span>
        <div class="modal-content" onclick="event.stopPropagation()">
            <div class="modal-info" id="modalInfo"></div>
            <img id="modalImage" class="modal-image" src="" alt="Full size image">
        </div>
    </div>
    
    <script>
        // 场景折叠/展开
        function toggleScene(sceneId) {{
            const content = document.getElementById('scene-' + sceneId);
            const header = content.previousElementSibling;
            
            if (content.classList.contains('hidden')) {{
                content.classList.remove('hidden');
                header.classList.remove('collapsed');
            }} else {{
                content.classList.add('hidden');
                header.classList.add('collapsed');
            }}
        }}
        
        // 显示全屏图片
        function showFullImage(imageSrc, sceneId, frameIndex) {{
            const modal = document.getElementById('imageModal');
            const modalImg = document.getElementById('modalImage');
            const modalInfo = document.getElementById('modalInfo');
            
            modal.classList.add('active');
            modalImg.src = imageSrc;
            modalInfo.textContent = `场景: ${{sceneId}} | 帧: ${{frameIndex}}`;
        }}
        
        // 关闭模态框
        function closeModal() {{
            const modal = document.getElementById('imageModal');
            modal.classList.remove('active');
        }}
        
        // ESC键关闭
        document.addEventListener('keydown', function(event) {{
            if (event.key === 'Escape') {{
                closeModal();
            }}
        }});
    </script>
</body>
</html>
        """
        
        return html
    
    def generate_html_report(
        self,
        images_dict: Dict[str, List[ImageFrame]],
        output_path: str,
        title: str = "场景图片查看器",
        thumbnail_size: int = 200
    ) -> str:
        """生成HTML报告
        
        Args:
            images_dict: 场景ID到ImageFrame列表的映射
            output_path: 输出HTML文件路径
            title: 报告标题
            thumbnail_size: 缩略图最大尺寸（像素）
            
        Returns:
            生成的HTML文件路径
            
        Example:
            >>> viewer = SceneImageHTMLViewer()
            >>> images = retriever.batch_load_images(scene_ids, frames_per_scene=3)
            >>> report_path = viewer.generate_html_report(
            ...     images, 
            ...     "cluster_report.html",
            ...     title="聚类分析图片"
            ... )
        """
        logger.info(f"生成HTML报告: {output_path}")
        logger.info(f"  场景数: {len(images_dict)}")
        
        total_frames = sum(len(frames) for frames in images_dict.values())
        logger.info(f"  总帧数: {total_frames}")
        
        # 生成HTML内容
        html_content = self._generate_html_template(
            images_dict, title, thumbnail_size
        )
        
        # 写入文件
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # 计算文件大小
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        logger.info(f"✅ HTML报告已生成: {output_file}")
        logger.info(f"   文件大小: {file_size_mb:.2f} MB")
        
        if file_size_mb > 100:
            logger.warning(f"⚠️ HTML文件较大 ({file_size_mb:.1f}MB)，建议减少帧数或场景数")
        
        return str(output_file.resolve())


# 便捷函数
def quick_generate_report(
    images_dict: Dict[str, List[ImageFrame]],
    output_path: str = None,
    title: str = "场景图片查看器"
) -> str:
    """快速生成HTML报告的便捷函数
    
    Args:
        images_dict: 场景图片字典
        output_path: 输出路径（默认自动生成）
        title: 报告标题
        
    Returns:
        生成的HTML文件路径
    """
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"scene_images_{timestamp}.html"
    
    viewer = SceneImageHTMLViewer()
    return viewer.generate_html_report(images_dict, output_path, title)

