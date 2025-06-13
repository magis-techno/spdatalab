"""
测试高德地图URL解析
"""

from spdatalab.routes.amap_utils import AmapRouteParser

def test_short_url():
    """测试短链接解析"""
    parser = AmapRouteParser()
    url = "https://surl.amap.com/25cT71TW75Y"
    
    # 展开短链接
    expanded_url = parser.expand_short_url(url)
    print(f"Expanded URL: {expanded_url}")
    
    # 提取坐标
    coords = parser.extract_coordinates_from_url(url)
    if coords:
        start, end = coords
        print(f"Start coordinates: {start}")
        print(f"End coordinates: {end}")
    
    # 获取路线信息
    route_info = parser.get_route_coordinates(url)
    if route_info:
        print(f"Route info: {route_info}")

if __name__ == "__main__":
    test_short_url() 