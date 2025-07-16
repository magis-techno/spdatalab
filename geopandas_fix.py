#!/usr/bin/env python3
"""
GeoPandas兼容性修复：移除to_postgis()中不支持的method参数
"""

def fix_geopandas_method_issue():
    """修复GeoPandas to_postgis method参数问题"""
    
    print("🔧 修复GeoPandas to_postgis method参数问题...")
    
    # 读取原文件
    with open('src/spdatalab/dataset/polygon_trajectory_query.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 查找并替换有问题的to_postgis调用
    old_code = """                # 批量插入到数据库
                gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    if_exists='append', 
                    index=False,
                    method='multi'  # 使用批量插入优化
                )"""
    
    new_code = """                # 批量插入到数据库（移除不支持的method参数）
                gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    if_exists='append', 
                    index=False
                )"""
    
    # 应用修复
    if old_code in content:
        content = content.replace(old_code, new_code)
        
        # 写回文件
        with open('src/spdatalab/dataset/polygon_trajectory_query.py', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ GeoPandas修复已应用")
        return True
    else:
        # 尝试更灵活的匹配
        import re
        pattern = r"gdf\.to_postgis\(\s*table_name,\s*self\.engine,\s*if_exists='append',\s*index=False,\s*method='multi'[^)]*\)"
        replacement = """gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    if_exists='append', 
                    index=False
                )"""
        
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE | re.DOTALL)
        
        if new_content != content:
            with open('src/spdatalab/dataset/polygon_trajectory_query.py', 'w', encoding='utf-8') as f:
                f.write(new_content)
            print("✅ GeoPandas修复已应用（正则匹配）")
            return True
        else:
            print("❌ 未找到需要修复的to_postgis调用")
            return False

def check_geopandas_version():
    """检查GeoPandas版本信息"""
    try:
        import geopandas as gpd
        print(f"📊 GeoPandas版本: {gpd.__version__}")
        
        # 检查to_postgis方法支持的参数
        import inspect
        sig = inspect.signature(gpd.GeoDataFrame.to_postgis)
        params = list(sig.parameters.keys())
        print(f"📋 to_postgis支持的参数: {params}")
        
        if 'method' in params:
            print("✅ 当前版本支持method参数")
        else:
            print("⚠️ 当前版本不支持method参数")
            
    except ImportError:
        print("❌ 无法导入GeoPandas")
    except Exception as e:
        print(f"❌ 检查GeoPandas时出错: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🔧 GeoPandas兼容性修复工具")
    print("=" * 60)
    
    # 检查版本
    check_geopandas_version()
    
    print("\n" + "=" * 60)
    
    # 应用修复
    success = fix_geopandas_method_issue()
    
    if success:
        print("\n🎉 修复完成！")
        print("\n建议运行命令：")
        print("python src/spdatalab/dataset/polygon_trajectory_query.py \\")
        print("  --input data/uturn_poi_20250716.geojson \\")
        print("  --table utrun_polygon_of_interest_trajectires \\")
        print("  --limit 500 \\")
        print("  --verbose")
    else:
        print("\n❌ 修复失败")
        print("请手动移除to_postgis()调用中的method='multi'参数") 