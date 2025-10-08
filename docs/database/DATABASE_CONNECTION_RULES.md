# 数据库连接使用规则

## 🚨 重要规则：严格按照以下规范使用数据库连接

### 1. Hive数据库连接 - 使用 `hive_cursor()`

#### ✅ 正确使用场景

**查询轨迹点数据** - 使用 `dataset_gy1` catalog：
```python
from spdatalab.common.io_hive import hive_cursor

# 查询轨迹点
with hive_cursor("dataset_gy1") as cur:
    cur.execute("""
        SELECT longitude, latitude, timestamp, dataset_name
        FROM ods_location_data 
        WHERE ST_Within(ST_Point(longitude, latitude), ST_GeomFromText(%s, 4326))
    """, (polygon_wkt,))
```

**查询业务数据** - 使用默认 `app_gy1` catalog：
```python
# 查询scene_id映射、event_id等业务数据
with hive_cursor() as cur:  # 默认使用app_gy1
    cur.execute("""
        SELECT origin_name AS data_name, id AS scene_id, event_id, event_name
        FROM transform.ods_t_data_fragment_datalake 
        WHERE origin_name IN %(tok)s
    """, {"tok": tuple(data_names)})
```

### 2. 本地PostgreSQL连接 - 使用 `create_engine()`

#### ✅ 正确使用场景

**仅用于本地PostgreSQL操作**：
```python
from sqlalchemy import create_engine

# 连接本地PostgreSQL
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
engine = create_engine(LOCAL_DSN, future=True)

# 保存结果到本地数据库
with engine.connect() as conn:
    gdf.to_postgis('result_table', conn, if_exists='append', index=False)
```

### 3. 数据库分工明确

| 数据库类型 | 连接方式 | Catalog | 用途 |
|------------|----------|---------|------|
| Hive | `hive_cursor("dataset_gy1")` | dataset_gy1 | 轨迹点数据查询 |
| Hive | `hive_cursor()` | app_gy1 (默认) | 业务数据查询 (scene_id, event_id等) |
| PostgreSQL | `create_engine(LOCAL_DSN)` | - | 结果存储和本地表操作 |

### 4. ❌ 禁止的错误用法

```python
# ❌ 错误：不要用create_engine连接Hive
engine = create_engine("hive://...")  # 禁止！

# ❌ 错误：不要用hive_cursor连接PostgreSQL  
with hive_cursor() as cur:
    cur.execute("CREATE TABLE local_table...")  # 禁止！

# ❌ 错误：catalog使用错误
with hive_cursor("app_gy1") as cur:
    cur.execute("SELECT * FROM ods_location_data")  # 应该用dataset_gy1！
```

### 5. 代码示例模板

#### 标准查询模式：
```python
def query_trajectories_in_polygon(polygon_wkt: str) -> pd.DataFrame:
    # 1. 查询轨迹点 (使用dataset_gy1)
    with hive_cursor("dataset_gy1") as cur:
        cur.execute(trajectory_sql, (polygon_wkt,))
        points_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    
    # 2. 查询business数据 (使用默认app_gy1)
    data_names = points_df['dataset_name'].unique().tolist()
    with hive_cursor() as cur:
        cur.execute(business_sql, {"tok": tuple(data_names)})
        business_df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    
    # 3. 合并数据并保存到本地PostgreSQL
    result_gdf = merge_and_process(points_df, business_df)
    
    # 4. 保存结果 (使用local PostgreSQL)
    with engine.connect() as conn:
        result_gdf.to_postgis('result_table', conn, if_exists='append', index=False)
    
    return result_gdf
```

### 6. 检查清单

在写代码时，请检查：
- [ ] 查询轨迹点数据时使用了 `hive_cursor("dataset_gy1")`
- [ ] 查询业务数据时使用了 `hive_cursor()` (默认app_gy1)
- [ ] 本地PostgreSQL操作使用了 `create_engine(LOCAL_DSN)`
- [ ] 没有混用连接方式
- [ ] SQL语句使用了正确的catalog下的表

### 7. 常见错误和解决方案

| 错误症状 | 可能原因 | 解决方案 |
|----------|----------|----------|
| 表不存在错误 | catalog使用错误 | 检查是否用对了dataset_gy1/app_gy1 |
| 连接超时 | 使用了错误的连接方式 | 改为使用hive_cursor |
| 数据类型错误 | PostgreSQL和Hive混用 | 明确区分本地和远程操作 |

---

**🔥 记住：Hive用hive_cursor，PostgreSQL用create_engine，绝不混用！** 