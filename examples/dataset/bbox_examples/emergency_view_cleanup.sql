-- 紧急视图清理脚本
-- 用于处理卡死的clips_bbox_unified_qgis视图

-- 1. 终止所有相关查询
SELECT 'Terminating related queries...' as status;
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE query LIKE '%clips_bbox_unified_qgis%' 
  AND pid != pg_backend_pid()
  AND state != 'idle';

-- 等待一下
SELECT pg_sleep(2);

-- 2. 检查视图是否存在
SELECT 'Checking view existence...' as status;
SELECT 
    CASE 
        WHEN EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'clips_bbox_unified_qgis')
        THEN 'View exists'
        ELSE 'View does not exist'
    END as view_status;

-- 3. 尝试简单删除
SELECT 'Attempting simple drop...' as status;
DROP VIEW IF EXISTS clips_bbox_unified_qgis;

-- 4. 验证删除结果
SELECT 'Verifying deletion...' as status;
SELECT 
    CASE 
        WHEN EXISTS(SELECT 1 FROM pg_views WHERE viewname = 'clips_bbox_unified_qgis')
        THEN 'ERROR: View still exists!'
        ELSE 'SUCCESS: View deleted'
    END as final_status;

-- 5. 清理任何残留的相关对象
DELETE FROM pg_depend 
WHERE objid IN (
    SELECT oid FROM pg_class WHERE relname = 'clips_bbox_unified_qgis'
);

-- 6. 显示剩余的bbox视图
SELECT 'Remaining bbox views:' as info;
SELECT viewname, schemaname 
FROM pg_views 
WHERE viewname LIKE '%bbox%' 
ORDER BY viewname;
