"""pytest配置文件"""

def pytest_addoption(parser):
    """添加命令行参数"""
    parser.addoption("--run-obs-tests", action="store_true", help="运行OBS相关的集成测试")
    parser.addoption("--run-local-tests", action="store_true", help="运行本地文件相关的集成测试") 