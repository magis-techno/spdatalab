-- 创建路线主表
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL COMMENT '数据来源（如：amap）',
    route_id VARCHAR(100) NOT NULL COMMENT '来源系统的路线ID',
    url VARCHAR(500) NOT NULL COMMENT '原始路线URL',
    name VARCHAR(200) COMMENT '路线名称/描述',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    metadata JSONB COMMENT '额外元数据',
    UNIQUE KEY uk_source_route (source, route_id)
) COMMENT '路线主表';

-- 创建路线分段表
CREATE TABLE IF NOT EXISTS route_segments (
    id SERIAL PRIMARY KEY,
    route_id INTEGER NOT NULL COMMENT '关联的路线ID',
    segment_id VARCHAR(100) NOT NULL COMMENT '分段ID',
    geometry GEOMETRY(LINESTRING, 4326) NOT NULL COMMENT '分段几何信息',
    properties JSONB COMMENT '分段属性信息',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (route_id) REFERENCES routes(id) ON DELETE CASCADE,
    UNIQUE KEY uk_route_segment (route_id, segment_id)
) COMMENT '路线分段表';

-- 创建索引
CREATE INDEX idx_routes_source ON routes(source);
CREATE INDEX idx_routes_route_id ON routes(route_id);
CREATE INDEX idx_segments_route_id ON route_segments(route_id);
CREATE INDEX idx_segments_geometry ON route_segments USING GIST(geometry); 