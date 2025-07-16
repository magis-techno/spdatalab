#!/usr/bin/env python3
"""
事务修复脚本：解决 polygon_trajectory_query 中的事务冲突问题
"""

def fix_transaction_issue():
    """修复事务冲突问题"""
    
    # 读取原文件
    with open('src/spdatalab/dataset/polygon_trajectory_query.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 查找并替换有问题的事务代码
    old_transaction_code = """                # 执行SQL（事务包装）
                trans = conn.begin()
                try:
                    conn.execute(create_sql)
                    conn.execute(add_geom_sql)
                    conn.execute(index_sql)
                    trans.commit()
                    
                    logger.info(f"✅ 轨迹表创建成功: {table_name}")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e"""
    
    new_transaction_code = """                # 分步执行SQL（避免事务冲突）
                try:
                    # 步骤1：创建表
                    conn.execute(create_sql)
                    conn.commit()
                    
                    # 步骤2：添加几何列
                    conn.execute(add_geom_sql)
                    conn.commit()
                    
                    # 步骤3：创建索引
                    conn.execute(index_sql)
                    conn.commit()
                    
                    logger.info(f"✅ 轨迹表创建成功: {table_name}")
                    return True
                except Exception as e:
                    logger.error(f"SQL执行失败: {str(e)}")
                    # 尝试清理部分创建的表
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        conn.commit()
                    except:
                        pass
                    raise e"""
    
    # 应用修复
    if old_transaction_code in content:
        content = content.replace(old_transaction_code, new_transaction_code)
        
        # 写回文件
        with open('src/spdatalab/dataset/polygon_trajectory_query.py', 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ 事务修复已应用")
        return True
    else:
        print("❌ 未找到需要修复的事务代码")
        return False

if __name__ == "__main__":
    success = fix_transaction_issue()
    if success:
        print("🎉 修复完成！现在可以重新运行 polygon_trajectory_query")
        print("\n建议运行命令：")
        print("python src/spdatalab/dataset/polygon_trajectory_query.py \\")
        print("  --input data/uturn_poi_20250716.geojson \\")
        print("  --table utrun_polygon_of_interest_trajectires \\")
        print("  --limit 500 \\")
        print("  --verbose")
    else:
        print("❌ 修复失败，请手动检查代码") 