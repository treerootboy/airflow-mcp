import asyncio
import re
from typing import Any
import httpx
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from pydantic import AnyUrl
import mcp.server.stdio
from datetime import datetime

# Initialize server
server = Server("airflow")

# Constants
AIRFLOW_API_BASE = "http://localhost:8080/api/v1"
AUTH = ("admin", "admin")
HEADERS = {
    "Accept": "application/json"
}

def analyze_logs(logs: str) -> dict:
    """分析任务日志，查找可能的错误原因。
    
    返回一个字典，包含：
    - error_type: 错误类型
    - error_message: 具体错误信息
    - suggestions: 建议的解决方案
    """
    error_patterns = {
        "import_error": (r"ImportError: (.*)", "导入错误", "检查依赖是否正确安装"),
        "permission_error": (r"PermissionError: (.*)", "权限错误", "检查文件/目录权限设置"),
        "connection_error": (r"ConnectionError: (.*)", "连接错误", "检查网络连接和服务可用性"),
        "timeout_error": (r"TimeoutError: (.*)", "超时错误", "考虑增加超时时间或优化任务性能"),
        "syntax_error": (r"SyntaxError: (.*)", "语法错误", "检查代码语法"),
        "key_error": (r"KeyError: (.*)", "键错误", "检查数据结构和字典键"),
        "value_error": (r"ValueError: (.*)", "值错误", "检查输入参数的有效性"),
        "operational_error": (r"OperationalError: (.*)", "数据库操作错误", "检查数据库连接和SQL语句"),
    }
    
    # 查找任务状态
    task_status = "SUCCESS" if "Marking task as SUCCESS" in logs else "FAILED"
    
    # 如果任务成功但DAG失败，可能是配置问题
    if task_status == "SUCCESS":
        return {
            "error_type": "Configuration Error",
            "error_message": "Task succeeded but DAG marked as failed",
            "suggestions": [
                "检查DAG的成功条件配置",
                "检查任务间的依赖关系",
                "检查自定义的回调或sensor设置"
            ]
        }
    
    # 查找已知错误模式
    for error_type, (pattern, label, suggestion) in error_patterns.items():
        if match := re.search(pattern, logs):
            return {
                "error_type": label,
                "error_message": match.group(1),
                "suggestions": [suggestion]
            }
    
    # 查找通用错误信息
    if "ERROR" in logs:
        error_lines = [line for line in logs.split('\n') if "ERROR" in line]
        if error_lines:
            return {
                "error_type": "未分类错误",
                "error_message": error_lines[0],
                "suggestions": ["查看完整日志以获取详细信息"]
            }
    
    return {
        "error_type": "Unknown Error",
        "error_message": "No specific error found in logs",
        "suggestions": ["检查完整日志", "检查DAG配置", "检查任务定义"]
    }

async def make_airflow_request(url: str, method: str = "GET", json: dict = None) -> dict[str, Any] | None:
    """Make a request to the Airflow API with proper error handling."""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.request(
                method,
                url,
                headers=HEADERS,
                auth=AUTH,
                json=json,
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    """List available prompts."""
    return [
        types.Prompt(
            name="summarize-dags",
            description="Creates a summary of all DAGs",
            arguments=[
                types.PromptArgument(
                    name="style",
                    description="Style of the summary (brief/detailed)",
                    required=False,
                )
            ],
        )
    ]

@server.get_prompt()
async def handle_get_prompt(
    name: str, arguments: dict[str, str] | None
) -> types.GetPromptResult:
    """Generate a prompt for summarizing DAGs."""
    if name != "summarize-dags":
        raise ValueError(f"Unknown prompt: {name}")

    style = (arguments or {}).get("style", "brief")
    detail_prompt = " Give extensive details." if style == "detailed" else ""

    dags = await list_dags()
    return types.GetPromptResult(
        description="Summarize the current DAGs",
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=f"Here are the current DAGs to summarize:{detail_prompt}\n\n{dags}",
                ),
            )
        ],
    )

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools."""
    return [
        types.Tool(
            name="trigger-dag",
            description="Trigger a DAG run",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "conf": {"type": "object", "required": False},
                },
                "required": ["dag_id"],
            },
        ),
        types.Tool(
            name="enable-dag",
            description="Enable a DAG",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"}
                },
                "required": ["dag_id"],
            },
        ),
        types.Tool(
            name="get-daily-report",
            description="Get a report for DAG runs within specified time range",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (default: today)",
                        "required": False
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days to include (default: 1, max: 30)",
                        "minimum": 1,
                        "maximum": 30,
                        "required": False
                    }
                },
                "required": ["dag_id"]
            },
        ),
        types.Tool(
            name="list-dags",
            description="List all available DAGs",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        types.Tool(
            name="get-dag-status",
            description="Get the status of a specific DAG's runs",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of runs to return",
                        "default": 1,
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number of runs to skip",
                        "default": 0,
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Field to order by (e.g. -start_date, start_date)",
                        "default": "-start_date",
                    },
                    "execution_date_gte": {
                        "type": "string",
                        "description": "Minimum execution date",
                        "required": False,
                    },
                    "execution_date_lte": {
                        "type": "string",
                        "description": "Maximum execution date",
                        "required": False,
                    },
                    "state": {
                        "type": "string",
                        "description": "Filter by state (success, running, failed, etc.)",
                        "required": False,
                    },
                },
                "required": ["dag_id"],
            },
        ),
        types.Tool(
            name="get-dag-logs",
            description="Get logs for a DAG run",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "run_id": {"type": "string"},
                    "task_id": {
                        "type": "string",
                        "description": "Get logs for specific task",
                        "required": False,
                    },
                    "try_number": {
                        "type": "integer",
                        "description": "The try number of the task execution",
                        "required": False,
                    },
                },
                "required": ["dag_id", "run_id"],
            },
        ),
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool execution requests."""
    if not arguments:
        arguments = {}

    if name == "list-dags":
        dags = await list_dags()
        return [
            types.TextContent(
                type="text",
                text=dags
            )
        ]

    if name == "get-dag-status":
        dag_id = arguments.get("dag_id")
        if not dag_id:
            raise ValueError("Missing dag_id")

        status = await get_dag_status(
            dag_id,
            limit=arguments.get("limit", 1),
            offset=arguments.get("offset", 0),
            order_by=arguments.get("order_by", "-start_date"),
            execution_date_gte=arguments.get("execution_date_gte"),
            execution_date_lte=arguments.get("execution_date_lte"),
            state=arguments.get("state")
        )
        return [
            types.TextContent(
                type="text",
                text=status
            )
        ]

    if name == "get-dag-logs":
        dag_id = arguments.get("dag_id")
        run_id = arguments.get("run_id")
        if not dag_id or not run_id:
            raise ValueError("Missing dag_id or run_id")

        logs = await get_dag_logs(
            dag_id,
            run_id,
            task_id=arguments.get("task_id"),
            try_number=arguments.get("try_number")
        )
        return [
            types.TextContent(
                type="text",
                text=logs
            )
        ]

    if name == "trigger-dag":
        dag_id = arguments.get("dag_id")
        if not dag_id:
            raise ValueError("Missing dag_id")

        conf = arguments.get("conf")
        url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
        data = await make_airflow_request(url, method="POST", json={"conf": conf or {}})
        
        if not data:
            raise ValueError(f"Failed to trigger DAG {dag_id}.")

        return [
            types.TextContent(
                type="text",
                text=f"""
Successfully triggered DAG {dag_id}
Run ID: {data["dag_run_id"]}
State: {data["state"]}
"""
            )
        ]

    if name == "enable-dag":
        dag_id = arguments.get("dag_id")
        if not dag_id:
            raise ValueError("Missing dag_id")

        url = f"{AIRFLOW_API_BASE}/dags/{dag_id}"
        data = await make_airflow_request(url, method="PATCH", json={"is_paused": False})
        
        if not data:
            raise ValueError(f"Failed to enable DAG {dag_id}.")

        return [
            types.TextContent(
                type="text",
                text=f"Successfully enabled DAG {dag_id}"
            )
        ]

    if name == "get-daily-report":
        dag_id = arguments.get("dag_id")
        if not dag_id:
            raise ValueError("Missing dag_id")

        start_date = arguments.get("start_date")
        days = int(arguments.get("days", 1))
        report = await get_daily_report(dag_id, start_date, days)
        return [
            types.TextContent(
                type="text",
                text=report
            )
        ]

    raise ValueError(f"Unknown tool: {name}")

async def list_dags() -> str:
    """List all available DAGs."""
    url = f"{AIRFLOW_API_BASE}/dags"
    data = await make_airflow_request(url)
    
    if not data or "dags" not in data:
        return "Unable to fetch DAGs."
    
    if not data["dags"]:
        return "No DAGs found."
    
    dags = [dag["dag_id"] for dag in data["dags"]]
    return "\n".join(dags)

async def get_dag_status(
    dag_id: str,
    limit: int = 1,
    offset: int = 0,
    order_by: str = "-start_date",
    execution_date_gte: str | None = None,
    execution_date_lte: str | None = None,
    state: str | None = None
) -> str:
    """Get the status of a specific DAG."""
    params = {
        "limit": limit,
        "offset": offset,
        "order_by": order_by
    }
    
    if execution_date_gte:
        params["execution_date_gte"] = execution_date_gte
    if execution_date_lte:
        params["execution_date_lte"] = execution_date_lte
    if state:
        params["state"] = state
    
    query_params = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns?{query_params}"
    data = await make_airflow_request(url)
    
    if not data or "dag_runs" not in data:
        return f"Unable to fetch status for DAG {dag_id}."
    
    if not data["dag_runs"]:
        return f"No runs found for DAG {dag_id}."
    
    result = [f"DAG: {dag_id}"]
    result.append(f"Total Runs: {len(data['dag_runs'])}")
    result.append("-" * 40)
    
    for run in data["dag_runs"]:
        result.extend([
            f"Run ID: {run['dag_run_id']}",
            f"State: {run['state']}",
            f"Start Date: {run['start_date']}",
            f"End Date: {run['end_date'] or 'N/A'}",
            "-" * 40
        ])
    
    return "\n".join(result)

async def get_dag_logs(
    dag_id: str,
    run_id: str,
    task_id: str | None = None,
    try_number: int | None = None
) -> str:
    """Get logs for a DAG run."""
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
    data = await make_airflow_request(url)
    
    if not data or "task_instances" not in data:
        return f"Unable to fetch task instances for DAG {dag_id} run {run_id}."
    
    if not data["task_instances"]:
        return f"No task instances found for DAG {dag_id} run {run_id}."
    
    result = []
    tasks = data["task_instances"]
    
    if task_id:
        tasks = [task for task in tasks if task["task_id"] == task_id]
        if not tasks:
            return f"Task {task_id} not found in DAG {dag_id} run {run_id}."
    
    for task in tasks:
        task_id = task["task_id"]
        attempt = try_number if try_number is not None else task["try_number"]
        if attempt < 1:
            attempt = 1
        
        log_url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{attempt}"
        response = await make_airflow_request(log_url)
        
        if not response or "content" not in response:
            result.append(f"Unable to fetch logs for task {task_id} attempt {attempt}")
            continue
        
        result.extend([
            f"=== Logs for task: {task_id} (attempt {attempt}) ===",
            response["content"],
            "=" * 50
        ])
    
    return "\n".join(result) if result else "No logs found."

async def get_daily_report(
    dag_id: str,
    start_date: str | None = None,
    days: int = 1
) -> str:
    """Get a DAG report for specified date range with execution summary and failure analysis."""
    if days < 1 or days > 30:
        return "Days parameter must be between 1 and 30"

    if not start_date:
        start_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    try:
        # 解析起始日期
        parsed_date = datetime.strptime(start_date, "%Y-%m-%d")
        # 计算日期范围
        from datetime import timedelta
        date_start = parsed_date.strftime("%Y-%m-%dT00:00:00+00:00")
        date_end = (parsed_date + timedelta(days=days)).strftime("%Y-%m-%dT23:59:59+00:00")
    except ValueError:
        return f"Invalid date format: {start_date}. Please use YYYY-MM-DD format."

    # 直接从API获取运行记录
    params = {
        "dag_id": dag_id,
        "page_size": 100,
        "start_date_gte": date_start,
        "start_date_lte": date_end,
        "order_by": "-start_date"
    }
    
    # 构建URL参数
    from urllib.parse import urlencode
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
    query_string = urlencode(params)
    data = await make_airflow_request(f"{url}?{query_string}")
    
    if not data or "dag_runs" not in data:
        return f"Unable to fetch runs for DAG {dag_id} on {date}"
    
    # 处理运行记录
    all_runs = data["dag_runs"]
    failed_runs = [run for run in all_runs if run["state"] == "failed"]
    
    # 生成运行统计
    stats = {
        "total": len(all_runs),
        "success": len([run for run in all_runs if run["state"] == "success"]),
        "failed": len(failed_runs),
        "running": len([run for run in all_runs if run["state"] == "running"]),
        "other": len([run for run in all_runs if run["state"] not in ["success", "failed", "running"]])
    }

    # 组装报告
    result = [
        f"Daily Report for DAG: {dag_id}",
        f"Date: {date}",
        "=" * 50,
        "\nExecution Summary:",
        f"Total Runs: {stats['total']}",
        f"Successful: {stats['success']}",
        f"Failed: {stats['failed']}",
        f"Running: {stats['running']}",
        f"Other States: {stats['other']}",
        "-" * 50
    ]

    # 如果有失败的运行，添加详细分析
    if failed_runs:
        result.extend([
            "\nFailed Runs Analysis:",
            "=" * 50
        ])

        for run in failed_runs:
            result.extend([
                f"\nRun ID: {run['dag_run_id']}",
                f"Start Time: {run['start_date']}",
                f"End Time: {run['end_date'] or 'N/A'}"
            ])
            
            # 获取该运行的所有任务日志
            logs = await get_dag_logs(dag_id, run['dag_run_id'])
            
            # 分析每个任务的日志
            for task_section in logs.split("=== Logs for task: "):
                if not task_section.strip():
                    continue
                
                # 提取任务名称和日志内容
                task_header, *rest = task_section.split(" ===")
                task_name = task_header.strip()
                task_logs = "===".join(rest)
                
                # 分析日志内容
                analysis = analyze_logs(task_logs)
                
                result.extend([
                    f"\nTask: {task_name}",
                    f"Status: {analysis['error_type']}",
                    f"Details: {analysis['error_message']}",
                    "\nRecommendations:",
                    *[f"- {suggestion}" for suggestion in analysis['suggestions']],
                    "-" * 40
                ])

    return "\n".join(result)

async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="airflow",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())