from spdatalab.routes.amap_utils import AmapRouteParser

def test_amap_url():
    # 测试短链接
    short_url = "https://surl.amap.com/25cT71TW75Y"
    print(f"\nTesting short URL: {short_url}")
    
    # 创建解析器实例
    parser = AmapRouteParser()
    
    # 1. 展开短链接
    expanded_url = parser.expand_short_url(short_url)
    print(f"\n1. Expanded URL:")
    print(expanded_url)
    
    # 2. 提取坐标
    coords = parser.extract_coordinates_from_url(short_url)
    if coords:
        start_coords, end_coords = coords
        print(f"\n2. Coordinates:")
        print(f"Start: {start_coords}")
        print(f"End: {end_coords}")
        
        # 3. 创建几何对象
        geometry = parser.create_geometry([start_coords, end_coords])
        if geometry:
            print(f"\n3. Geometry:")
            print(f"Type: {type(geometry).__name__}")
            print(f"Start point: {geometry.coords[0]}")
            print(f"End point: {geometry.coords[-1]}")
    else:
        print("\nFailed to extract coordinates")

if __name__ == "__main__":
    test_amap_url() 