-- 业务数据库初始化脚本
-- 创建元数据表，模拟 transform.ods_t_data_fragment_datalake

-- 创建schema
CREATE SCHEMA IF NOT EXISTS transform;
CREATE SCHEMA IF NOT EXISTS public;

-- 创建主要的元数据表
CREATE TABLE transform.ods_t_data_fragment_datalake (
    id VARCHAR(100) PRIMARY KEY,
    origin_name VARCHAR(255) NOT NULL,
    event_id VARCHAR(100),
    city_id VARCHAR(50),
    timestamp BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 额外字段
    data_type VARCHAR(50) DEFAULT 'trajectory',
    status VARCHAR(20) DEFAULT 'active',
    file_size BIGINT,
    processing_stage INTEGER DEFAULT 1
);

-- 创建索引
CREATE INDEX idx_ods_data_fragment_id ON transform.ods_t_data_fragment_datalake (id);
CREATE INDEX idx_ods_data_fragment_origin_name ON transform.ods_t_data_fragment_datalake (origin_name);
CREATE INDEX idx_ods_data_fragment_event_id ON transform.ods_t_data_fragment_datalake (event_id);
CREATE INDEX idx_ods_data_fragment_city_id ON transform.ods_t_data_fragment_datalake (city_id);
CREATE INDEX idx_ods_data_fragment_timestamp ON transform.ods_t_data_fragment_datalake (timestamp);

-- 创建城市映射表
CREATE TABLE public.city_mapping (
    city_id VARCHAR(50) PRIMARY KEY,
    city_name VARCHAR(100),
    region VARCHAR(50),
    country VARCHAR(50) DEFAULT 'China',
    timezone VARCHAR(50) DEFAULT 'Asia/Shanghai'
);

-- 插入城市数据
INSERT INTO public.city_mapping (city_id, city_name, region) VALUES
('BJ1', 'Beijing', 'North China'),
('SH1', 'Shanghai', 'East China'),
('GZ1', 'Guangzhou', 'South China'),
('SZ1', 'Shenzhen', 'South China'),
('A01', 'Mock City A01', 'Test Region'),
('A02', 'Mock City A02', 'Test Region'),
('A03', 'Mock City A03', 'Test Region'),
('A72', 'Mock City A72', 'Test Region'),
('B15', 'Mock City B15', 'Test Region');

-- 插入一些示例元数据
INSERT INTO transform.ods_t_data_fragment_datalake (
    id, origin_name, event_id, city_id, timestamp, data_type, file_size
) VALUES
('scene_001', 'test_dataset_001', 'mock_event_scene_00', 'BJ1', 1640995200000, 'trajectory', 1024000),
('scene_002', 'test_dataset_002', 'mock_event_scene_00', 'SH1', 1640995202000, 'trajectory', 2048000),
('scene_003', 'test_dataset_003', 'mock_event_scene_00', 'GZ1', 1640995204000, 'trajectory', 1536000),
('scene_004', 'test_dataset_004', 'mock_event_scene_00', 'SZ1', 1640995206000, 'trajectory', 1792000),
('scene_005', 'test_dataset_005', 'mock_event_scene_00', 'A01', 1640995208000, 'trajectory', 1280000);

-- 创建事件表（可选，用于更复杂的测试场景）
CREATE TABLE public.events (
    event_id VARCHAR(100) PRIMARY KEY,
    event_name VARCHAR(200),
    event_type VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    description TEXT,
    metadata JSONB
);

-- 插入事件数据
INSERT INTO public.events (event_id, event_name, event_type, start_time, end_time, description) VALUES
('mock_event_scene_00', 'Test Event 001', 'trajectory_collection', 
 '2022-01-01 00:00:00', '2022-01-01 23:59:59', 'Mock trajectory collection event'),
('mock_event_scene_01', 'Test Event 002', 'trajectory_collection',
 '2022-01-02 00:00:00', '2022-01-02 23:59:59', 'Mock trajectory collection event 2');

-- 创建数据质量统计视图
CREATE VIEW public.data_quality_stats AS
SELECT 
    city_id,
    COUNT(*) as total_scenes,
    COUNT(CASE WHEN processing_stage = 2 THEN 1 END) as processed_scenes,
    AVG(file_size) as avg_file_size,
    MIN(timestamp) as earliest_timestamp,
    MAX(timestamp) as latest_timestamp
FROM transform.ods_t_data_fragment_datalake
GROUP BY city_id;

-- 创建更新触发器
CREATE OR REPLACE FUNCTION update_business_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_ods_data_fragment_updated_at 
    BEFORE UPDATE ON transform.ods_t_data_fragment_datalake 
    FOR EACH ROW EXECUTE FUNCTION update_business_updated_at(); 