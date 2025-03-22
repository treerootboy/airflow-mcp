# Airflow MCP 服务器

## 项目概述
本项目是一个与 Apache Airflow 集成的 Model Context Protocol (MCP) 服务器，提供多种工具来管理和监控 Airflow 的 DAG 运行。它支持触发 DAG、获取 DAG 状态、回填数据、获取日志等操作。

## 功能特性
- **触发 DAG (`trigger-dag`)**: 触发指定 DAG 的运行。
- **启用 DAG (`enable-dag`)**: 启用指定的 DAG。
- **获取每日报告 (`get-daily-report`)**: 获取指定时间范围内的所有 DAG 运行的汇总报告。
- **列出所有 DAG (`list-dags`)**: 列出所有可用的 DAG。
- **批量获取 DAG 运行记录 (`list-dag-runs`)**: 批量获取 DAG 的运行记录。
- **获取 DAG 运行状态 (`get-dag-status`)**: 获取特定 DAG 的运行状态。
- **获取 DAG 日志 (`get-dag-logs`)**: 获取 DAG 运行的日志。
- **回填 DAG (`backfill-dag`)**: 回填指定日期范围内的 DAG 数据。

## 使用
```json
{
  "mcpServers": {
    "airflow": {
      "command": "uvx",
      "args": [
        "airflow-mcp"
      ]
    }
  }
}
```

## 开发
1. uv sync
2. 修改 mcp_config.json, 加入 airflow 配置
```json
{
  "mcpServers": {
    "airflow": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/airflow-mcp",
        "run",
        "airflow_mcp.py"
      ]
    }
  }
}
```
3. 启动 airflow 实例
```bash
mkdir ~/airflow-docker
cd ~/airflow-docker
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.3/docker-compose.yaml'
cat << EOF > .env
AIRFLOW_UID=$(id -u)
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__EXECUTOR=SequentialExecutor
EOF
docker compose up -d
```

## 贡献
欢迎贡献代码！请提交 Pull Request 并确保遵循项目的编码规范。

## 许可
本项目采用 MIT 许可协议。