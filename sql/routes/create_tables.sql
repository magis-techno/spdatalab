-- 创建路线主表
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    route_id VARCHAR(100) NOT NULL,
    url VARCHAR(500) NOT NULL,
    name VARCHAR(200),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- 添加表注释
COMMENT ON TABLE routes IS '路线主表';
COMMENT ON COLUMN routes.source IS '数据来源（如：amap）';
COMMENT ON COLUMN routes.route_id IS '来源系统的路线ID';
COMMENT ON COLUMN routes.url IS '原始路线URL';
COMMENT ON COLUMN routes.name IS '路线名称/描述';
COMMENT ON COLUMN routes.metadata IS '额外元数据';

-- 创建路线分段表
CREATE TABLE IF NOT EXISTS route_segments (
    id SERIAL PRIMARY KEY,
    route_id INTEGER NOT NULL,
    segment_id VARCHAR(100) NOT NULL,
    geometry GEOMETRY(LINESTRING, 4326) NOT NULL,
    properties JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (route_id) REFERENCES routes(id) ON DELETE CASCADE
);

-- 添加表注释
COMMENT ON TABLE route_segments IS '路线分段表';
COMMENT ON COLUMN route_segments.route_id IS '关联的路线ID';
COMMENT ON COLUMN route_segments.segment_id IS '分段ID';
COMMENT ON COLUMN route_segments.geometry IS '分段几何信息';
COMMENT ON COLUMN route_segments.properties IS '分段属性信息';

-- 创建索引
CREATE INDEX idx_routes_source ON routes(source);
CREATE INDEX idx_routes_route_id ON routes(route_id);
CREATE INDEX idx_segments_route_id ON route_segments(route_id);
CREATE INDEX idx_segments_geometry ON route_segments USING GIST(geometry);

-- 创建唯一约束
ALTER TABLE routes ADD CONSTRAINT uk_source_route UNIQUE (source, route_id);
ALTER TABLE route_segments ADD CONSTRAINT uk_route_segment UNIQUE (route_id, segment_id); 