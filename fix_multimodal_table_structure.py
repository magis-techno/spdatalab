#!/usr/bin/env python3
"""
多模态轨迹表结构修复脚本

此脚本用于修复多模态轨迹检索中的表结构问题：
- 在polygon_trajectory_query.py中添加多模态字段到表创建SQL
- 支持query_type, query_content, collection字段
- 自动备份和验证修复效果

问题背景：
多模态轨迹检索尝试插入query_type等字段，但表结构中没有这些列

修复策略：
在CREATE TABLE语句中添加多模态相关字段
"""

import re
import logging
import shutil
from pathlib import Path
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultimodalTableStructureFixer:
    """多模态表结构修复器"""
    
    def __init__(self):
        self.target_file = Path("src/spdatalab/dataset/polygon_trajectory_query.py")
        self.backup_suffix = f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
        
        # 需要添加的多模态字段
        self.multimodal_fields = [
            "query_type text",                    # 查询类型：'text' 或 'image'
            "query_content text",                 # 查询内容
            "collection varchar(255)",            # collection名称
            "source_polygons text"                # 源polygon信息
        ]
    
    def run_fix(self) -> bool:
        """运行表结构修复"""
        logger.info("🔧 开始多模态轨迹表结构修复...")
        
        if not self.target_file.exists():
            logger.error(f"目标文件不存在: {self.target_file}")
            return False
        
        try:
            # 1. 读取原始文件
            original_content = self.read_original_file()
            
            # 2. 创建备份
            backup_path = self.create_backup()
            logger.info(f"📁 备份创建: {backup_path}")
            
            # 3. 应用修复
            modified_content = self.apply_multimodal_fields_fix(original_content)
            
            # 4. 写入修复后的内容
            self.write_modified_file(modified_content)
            
            # 5. 验证修复效果
            if self.verify_fix():
                logger.info("✅ 多模态表结构修复成功！")
                return True
            else:
                logger.error("❌ 修复验证失败")
                return False
                
        except Exception as e:
            logger.error(f"修复过程失败: {e}")
            return False
    
    def read_original_file(self) -> str:
        """读取原始文件内容"""
        with open(self.target_file, 'r', encoding='utf-8') as f:
            return f.read()
    
    def create_backup(self) -> Path:
        """创建备份文件"""
        backup_path = self.target_file.with_suffix(self.backup_suffix)
        shutil.copy2(self.target_file, backup_path)
        return backup_path
    
    def apply_multimodal_fields_fix(self, content: str) -> str:
        """应用多模态字段修复"""
        logger.info("🔄 在CREATE TABLE语句中添加多模态字段...")
        
        # 查找CREATE TABLE语句的位置
        create_table_pattern = r'(CREATE TABLE \{table_name\} \(\s*\n.*?polygon_ids text\[\],\s*\n.*?created_at timestamp DEFAULT CURRENT_TIMESTAMP\s*\n\s*\)\s*;)'
        
        def replacement_func(match):
            original_sql = match.group(1)
            logger.info("找到CREATE TABLE语句，添加多模态字段...")
            
            # 在created_at之前插入多模态字段
            multimodal_fields_sql = ",\n                    ".join([
                "",  # 空字符串用于在开头添加逗号
                *self.multimodal_fields
            ])
            
            # 替换策略：在created_at行之前插入字段
            modified_sql = original_sql.replace(
                "created_at timestamp DEFAULT CURRENT_TIMESTAMP",
                f"{multimodal_fields_sql.lstrip(',')},\n                    created_at timestamp DEFAULT CURRENT_TIMESTAMP"
            )
            
            logger.info("✅ 多模态字段添加完成")
            return modified_sql
        
        # 执行替换
        modified_content = re.sub(create_table_pattern, replacement_func, content, flags=re.DOTALL)
        
        # 检查是否成功修改
        if modified_content == content:
            logger.warning("⚠️ 未检测到CREATE TABLE模式，尝试手动模式...")
            modified_content = self.apply_manual_fix(content)
        
        return modified_content
    
    def apply_manual_fix(self, content: str) -> str:
        """手动模式：直接替换特定行"""
        logger.info("🔄 使用手动模式修复...")
        
        # 查找特定的行并替换
        target_line = "                    created_at timestamp DEFAULT CURRENT_TIMESTAMP"
        
        if target_line in content:
            # 构建插入内容
            multimodal_lines = [
                "                    query_type text,",
                "                    query_content text,",
                "                    collection varchar(255),",
                "                    source_polygons text,",
                "                    created_at timestamp DEFAULT CURRENT_TIMESTAMP"
            ]
            
            replacement_lines = "\n".join(multimodal_lines)
            
            # 执行替换
            modified_content = content.replace(target_line, replacement_lines)
            
            logger.info("✅ 手动模式修复完成")
            return modified_content
        else:
            logger.error("❌ 无法找到目标行进行修复")
            return content
    
    def write_modified_file(self, content: str):
        """写入修复后的文件"""
        with open(self.target_file, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"📝 修复后文件已写入: {self.target_file}")
    
    def verify_fix(self) -> bool:
        """验证修复效果"""
        logger.info("🔍 验证修复效果...")
        
        with open(self.target_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查多模态字段是否已添加
        checks = [
            ("query_type text", "query_type字段"),
            ("query_content text", "query_content字段"),
            ("collection varchar", "collection字段"),
            ("source_polygons text", "source_polygons字段")
        ]
        
        all_passed = True
        for field_sql, field_name in checks:
            if field_sql in content:
                logger.info(f"   ✅ {field_name}已添加")
            else:
                logger.warning(f"   ❌ {field_name}未找到")
                all_passed = False
        
        return all_passed
    
    def generate_test_sql(self) -> str:
        """生成测试SQL"""
        return f"""
-- 测试多模态轨迹表创建
CREATE TABLE test_discovered_trajectories (
    id serial PRIMARY KEY,
    dataset_name text NOT NULL,
    scene_id text,
    event_id integer,
    event_name varchar(765),
    start_time bigint,
    end_time bigint,
    duration bigint,
    point_count integer,
    avg_speed numeric(8,2),
    max_speed numeric(8,2),
    min_speed numeric(8,2),
    std_speed numeric(8,2),
    avp_ratio numeric(5,3),
    polygon_ids text[],
    query_type text,
    query_content text,
    collection varchar(255),
    source_polygons text,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);

-- 添加几何列
SELECT AddGeometryColumn('public', 'test_discovered_trajectories', 'geometry', 4326, 'LINESTRING', 2);

-- 测试插入多模态数据
INSERT INTO test_discovered_trajectories 
(dataset_name, query_type, query_content, collection) 
VALUES 
('test_dataset', 'text', 'bicycle crossing intersection', 'ddi_collection_camera_encoded_1');

-- 查询验证
SELECT query_type, query_content, collection FROM test_discovered_trajectories;

-- 清理
DROP TABLE test_discovered_trajectories;
"""

def create_manual_sql_fix():
    """创建手动SQL修复脚本"""
    sql_script = """-- 手动SQL修复脚本：为existing表添加多模态字段

-- 1. 检查表是否存在
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'discovered_trajectories'
);

-- 2. 如果表存在，添加缺失字段
DO $$
BEGIN
    -- 添加query_type字段
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'discovered_trajectories' 
        AND column_name = 'query_type'
    ) THEN
        ALTER TABLE discovered_trajectories ADD COLUMN query_type text;
    END IF;
    
    -- 添加query_content字段
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'discovered_trajectories' 
        AND column_name = 'query_content'
    ) THEN
        ALTER TABLE discovered_trajectories ADD COLUMN query_content text;
    END IF;
    
    -- 添加collection字段
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'discovered_trajectories' 
        AND column_name = 'collection'
    ) THEN
        ALTER TABLE discovered_trajectories ADD COLUMN collection varchar(255);
    END IF;
    
    -- 添加source_polygons字段
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'discovered_trajectories' 
        AND column_name = 'source_polygons'
    ) THEN
        ALTER TABLE discovered_trajectories ADD COLUMN source_polygons text;
    END IF;
END $$;

-- 3. 验证字段是否添加成功
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name = 'discovered_trajectories'
AND column_name IN ('query_type', 'query_content', 'collection', 'source_polygons')
ORDER BY column_name;
"""
    
    with open('fix_existing_table_structure.sql', 'w', encoding='utf-8') as f:
        f.write(sql_script)
    
    logger.info("✅ 已创建手动SQL修复脚本: fix_existing_table_structure.sql")

def main():
    """主函数"""
    print("🔧 多模态轨迹表结构修复工具")
    print("="*50)
    
    # 创建修复器
    fixer = MultimodalTableStructureFixer()
    
    # 生成测试SQL
    test_sql = fixer.generate_test_sql()
    with open('test_multimodal_table.sql', 'w', encoding='utf-8') as f:
        f.write(test_sql)
    logger.info("✅ 已创建测试SQL脚本: test_multimodal_table.sql")
    
    # 创建手动SQL修复脚本（用于修复已存在的表）
    create_manual_sql_fix()
    
    # 运行修复
    success = fixer.run_fix()
    
    print("\n" + "="*60)
    print("🎯 下一步操作建议:")
    print("="*60)
    
    if success:
        print("✅ 代码修复成功！现在有两种方案处理数据库：")
        print("")
        print("方案A - 删除现有表（推荐，如果数据不重要）：")
        print("  1. 连接数据库删除现有表：")
        print("     DROP TABLE IF EXISTS discovered_trajectories;")
        print("  2. 重新运行多模态轨迹检索，会自动创建新表结构")
        print("")
        print("方案B - 修改现有表结构（保留已有数据）：")
        print("  1. 在数据库中执行SQL脚本：")
        print("     psql -U postgres -d postgres -f fix_existing_table_structure.sql")
        print("  2. 或手动执行SQL文件中的命令")
        print("")
        print("然后重新测试：")
        print("python -m spdatalab.fusion.multimodal_trajectory_retrieval \\")
        print("  --text 'bicycle crossing intersection' \\")
        print("  --collection 'ddi_collection_camera_encoded_1' \\")
        print("  --output-table 'discovered_trajectories' \\")
        print("  --verbose")
    else:
        print("❌ 代码修复失败，请检查错误信息")

if __name__ == "__main__":
    main()
