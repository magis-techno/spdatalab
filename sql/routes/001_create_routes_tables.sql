-- Create routes table
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    route_id VARCHAR(100) NOT NULL,
    url VARCHAR(500) NOT NULL,
    name VARCHAR(200),
    route_metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create route segments table
CREATE TABLE IF NOT EXISTS route_segments (
    id SERIAL PRIMARY KEY,
    route_id INTEGER REFERENCES routes(id),
    segment_id INTEGER NOT NULL,
    gaode_link VARCHAR(500) NOT NULL,
    route_points TEXT,
    segment_distance FLOAT,
    geometry GEOMETRY(LINESTRING, 4326),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(route_id, segment_id)
);

-- Create index on route_id
CREATE INDEX IF NOT EXISTS idx_route_segments_route_id ON route_segments(route_id);

-- Create index on geometry
CREATE INDEX IF NOT EXISTS idx_route_segments_geometry ON route_segments USING GIST(geometry); 