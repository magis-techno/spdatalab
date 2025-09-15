#!/usr/bin/env python3
"""
诊断parquet文件问题的脚本
"""

import sys
import pandas as pd
from pathlib import Path
import json

def diagnose_parquet_file(file_path):
    """诊断parquet文件的问题"""
    print(f"🔍 诊断parquet文件: {file_path}")
    
    file_path = Path(file_path)
    
    # 1. 检查文件是否存在
    if not file_path.exists():
        print(f"❌ 文件不存在: {file_path}")
        return False
    
    # 2. 检查文件大小
    file_size = file_path.stat().st_size
    print(f"📁 文件大小: {file_size} bytes ({file_size/1024:.2f} KB)")
    
    # 3. 检查文件头部
    try:
        with open(file_path, 'rb') as f:
            header = f.read(4)
            print(f"🔤 文件头部: {header}")
    except Exception as e:
        print(f"❌ 读取文件头部失败: {e}")
        return False
    
    # 4. 尝试读取parquet文件
    try:
        print("📖 尝试读取parquet文件...")
        df = pd.read_parquet(file_path)
        print(f"✅ 成功读取parquet文件!")
        print(f"   - 行数: {len(df)}")
        print(f"   - 列数: {len(df.columns)}")
        print(f"   - 列名: {list(df.columns)}")
        
        if 'scene_id' in df.columns:
            unique_scene_ids = df['scene_id'].nunique()
            print(f"   - 唯一scene_id数量: {unique_scene_ids}")
        
        # 显示前几行
        print("\n📋 前5行数据:")
        print(df.head())
        
        return True
        
    except Exception as e:
        print(f"❌ 读取parquet文件失败: {e}")
        print(f"   错误类型: {type(e).__name__}")
        return False

def test_bbox_load_function(file_path):
    """测试bbox模块的加载函数"""
    print(f"\n🧪 测试bbox加载函数...")
    
    try:
        # 导入bbox模块的函数
        sys.path.insert(0, 'src')
        from spdatalab.dataset.bbox import load_scene_ids_from_parquet
        
        scene_ids = load_scene_ids_from_parquet(file_path)
        print(f"✅ bbox加载成功! 加载了 {len(scene_ids)} 个scene_id")
        return True
        
    except Exception as e:
        print(f"❌ bbox加载失败: {e}")
        print(f"   错误类型: {type(e).__name__}")
        return False

def compare_with_meta_file(parquet_path):
    """比较parquet文件和meta文件的信息"""
    meta_path = Path(parquet_path).with_suffix('.meta.json')
    
    if meta_path.exists():
        print(f"\n📊 检查meta文件: {meta_path}")
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            
            print("Meta文件内容:")
            for key, value in meta_data.items():
                print(f"  {key}: {value}")
            
        except Exception as e:
            print(f"❌ 读取meta文件失败: {e}")
    else:
        print(f"⚠️ Meta文件不存在: {meta_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python diagnose_parquet.py <parquet_file_path>")
        sys.exit(1)
    
    parquet_file = sys.argv[1]
    
    print("=" * 60)
    print("🩺 Parquet文件诊断工具")
    print("=" * 60)
    
    # 诊断parquet文件
    success = diagnose_parquet_file(parquet_file)
    
    if success:
        # 测试bbox加载函数
        test_bbox_load_function(parquet_file)
    
    # 检查meta文件
    compare_with_meta_file(parquet_file)
    
    print("\n" + "=" * 60)
    print("🏁 诊断完成")
    print("=" * 60)
