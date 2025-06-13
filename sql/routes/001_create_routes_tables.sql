-- 启用PostGIS扩展
CREATE EXTENSION IF NOT EXISTS postgis;

-- 创建路线表
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,                    -- 路线名称
    region VARCHAR(100),                           -- 区域
    total_distance FLOAT,                          -- 总距离(米)
    is_active BOOLEAN DEFAULT true,                -- 是否激活
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建路线分段表
CREATE TABLE IF NOT EXISTS route_segments (
    id SERIAL PRIMARY KEY,
    route_id INTEGER REFERENCES routes(id),
    segment_order INTEGER NOT NULL,                -- 分段顺序
    gaode_link VARCHAR(500) NOT NULL,              -- 高德地图链接
    distance FLOAT,                                -- 分段距离(米)
    start_point GEOMETRY(POINT, 4326),            -- 起点坐标
    end_point GEOMETRY(POINT, 4326),              -- 终点坐标
    path GEOMETRY(LINESTRING, 4326),              -- 路线几何
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(route_id, segment_order)
);

-- 创建路线点表（存储详细的路径点）
CREATE TABLE IF NOT EXISTS route_points (
    id SERIAL PRIMARY KEY,
    segment_id INTEGER REFERENCES route_segments(id),
    point_order INTEGER NOT NULL,                  -- 点的顺序
    point GEOMETRY(POINT, 4326) NOT NULL,         -- 点的坐标
    elevation FLOAT,                               -- 海拔高度
    speed_limit FLOAT,                            -- 限速(km/h)
    road_type VARCHAR(50),                        -- 道路类型
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(segment_id, point_order)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_route_segments_route_id ON route_segments(route_id);
CREATE INDEX IF NOT EXISTS idx_route_segments_path ON route_segments USING GIST(path);
CREATE INDEX IF NOT EXISTS idx_route_points_segment_id ON route_points(segment_id);
CREATE INDEX IF NOT EXISTS idx_route_points_point ON route_points USING GIST(point); 