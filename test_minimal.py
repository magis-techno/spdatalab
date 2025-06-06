import time
from src.spdatalab.fusion.spatial_join_minimal import simple_polygon_intersect

print("测试最简单的polygon相交判断...")

start_time = time.time()
result = simple_polygon_intersect(num_bbox=5)
end_time = time.time()

print(f"结果:")
print(result)
print(f"\n总耗时: {end_time - start_time:.2f}秒")
print(f"平均每个bbox: {(end_time - start_time)/len(result):.2f}秒") 