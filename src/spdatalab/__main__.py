#!/usr/bin/env python3
"""
spdatalab命令行工具入口点

支持通过 python -m spdatalab 或 spdatalab 命令运行
"""

if __name__ == '__main__':
    from .cli import cli
    cli() 