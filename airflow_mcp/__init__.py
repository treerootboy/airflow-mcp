from . import airflow_mcp
import asyncio

def main():
    asyncio.run(airflow_mcp.main())
    
__all__ = ['main', 'airflow_mcp']