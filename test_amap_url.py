from spdatalab.routes.amap_utils import AmapRouteParser

def test_amap_url():
    # 测试短链接
    short_url = "https://surl.amap.com/25cT71TW75Y"
    print(f"\nTesting short URL: {short_url}")
    
    # 创建路线实例
    route = AmapRouteParser.create_route(short_url, "Test Route")
    
    # 打印路线信息
    print("\nRoute information:")
    print(f"Source: {route.source}")
    print(f"Route ID: {route.route_id}")
    print(f"URL: {route.url}")
    print(f"Name: {route.name}")
    
    if route.metadata:
        print("\nMetadata:")
        print(f"Distance: {route.metadata.get('distance')}")
        print(f"Duration: {route.metadata.get('duration')}")
        print(f"Number of steps: {len(route.metadata.get('steps', []))}")
    
    if route.geometry:
        print("\nGeometry:")
        print(f"Type: {type(route.geometry).__name__}")
        print(f"Number of points: {len(route.geometry.coords)}")
        print(f"Start point: {route.geometry.coords[0]}")
        print(f"End point: {route.geometry.coords[-1]}")

if __name__ == "__main__":
    test_amap_url() 