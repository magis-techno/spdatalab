CREATE TABLE IF NOT EXISTS scene_sets (
  set_id      SERIAL PRIMARY KEY,
  name        TEXT        NOT NULL,
  description TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scene_set_members (
  set_id      INT  REFERENCES scene_sets(set_id) ON DELETE CASCADE,
  scene_token TEXT NOT NULL,
  PRIMARY KEY (set_id, scene_token)
);