"""
配置管理模块
"""
import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @property
    def dsn(self) -> str:
        return f"postgresql+psycopg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class Config:
    """应用配置"""
    
    @staticmethod
    def get_local_db_config() -> DatabaseConfig:
        """获取本地数据库配置"""
        return DatabaseConfig(
            host=os.getenv("LOCAL_DB_HOST", "local_pg"),
            port=int(os.getenv("LOCAL_DB_PORT", "5432")),
            database=os.getenv("LOCAL_DB_NAME", "postgres"),
            username=os.getenv("LOCAL_DB_USER", "postgres"),
            password=os.getenv("LOCAL_DB_PASSWORD", "postgres")
        )
    
    @staticmethod
    def get_remote_db_config() -> DatabaseConfig:
        """获取远程数据库配置"""
        return DatabaseConfig(
            host=os.getenv("REMOTE_DB_HOST", "10.170.30.193"),
            port=int(os.getenv("REMOTE_DB_PORT", "9001")),
            database=os.getenv("REMOTE_DB_NAME", "rcdatalake_gy1"),
            username=os.getenv("REMOTE_DB_USER", "**"),
            password=os.getenv("REMOTE_DB_PASSWORD", "**")
        )
    
    @staticmethod
    def get_batch_config() -> Dict[str, Any]:
        """获取批处理配置"""
        return {
            "temp_table_prefix": os.getenv("TEMP_TABLE_PREFIX", "temp_bbox_batch"),
            "default_batch_size": int(os.getenv("DEFAULT_BATCH_SIZE", "1000")),
            "max_retries": int(os.getenv("MAX_RETRIES", "3")),
            "timeout_seconds": int(os.getenv("TIMEOUT_SECONDS", "300"))
        } 