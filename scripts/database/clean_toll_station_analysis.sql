-- 清理收费站分析相关的表和视图
-- 使用方法: psql -d spdatalab -f scripts/clean_toll_station_analysis.sql

-- 删除视图（如果存在）
DROP VIEW IF EXISTS toll_station_analysis_view CASCADE;
DROP VIEW IF EXISTS toll_station_trajectories_view CASCADE;
DROP VIEW IF EXISTS toll_station_summary_view CASCADE;
DROP VIEW IF EXISTS toll_station_qgis_view CASCADE;
DROP VIEW IF EXISTS toll_station_trajectory_qgis_view CASCADE;

-- 删除表（如果存在）
DROP TABLE IF EXISTS toll_station_trajectories CASCADE;
DROP TABLE IF EXISTS toll_station_analysis CASCADE;

-- 删除相关索引（如果存在）
DROP INDEX IF EXISTS idx_toll_station_analysis_analysis_id;
DROP INDEX IF EXISTS idx_toll_station_analysis_intersection_id;
DROP INDEX IF EXISTS idx_toll_station_trajectories_analysis_id;
DROP INDEX IF EXISTS idx_toll_station_trajectories_toll_station_id;
DROP INDEX IF EXISTS idx_toll_station_trajectories_dataset_name;

-- 删除相关序列（如果存在）
DROP SEQUENCE IF EXISTS toll_station_analysis_id_seq CASCADE;
DROP SEQUENCE IF EXISTS toll_station_trajectories_id_seq CASCADE;

-- 清理完成
SELECT '✅ 收费站分析相关表和视图已清理完成' as message; 