-- Create routes table
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    route_name VARCHAR(200) NOT NULL,
    region VARCHAR(100),
    total_distance FLOAT,
    is_active BOOLEAN DEFAULT true,
    allocation_count INTEGER DEFAULT 0,
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