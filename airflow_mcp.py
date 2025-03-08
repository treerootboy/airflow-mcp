import asyncio
import re
from typing import Any
import httpx
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from pydantic import AnyUrl
import mcp.server.stdio
from datetime import datetime, timedelta
import os

# Initialize server
server = Server("airflow")

# Constants
AIRFLOW_API_BASE = os.environ.get("AIRFLOW_API_BASE", "http://localhost:8080/api/v1")
AUTH = (
    os.environ.get("AIRFLOW_USERNAME", "admin"), 
    os.environ.get("AIRFLOW_PASSWORD", "admin")
)
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
        except Exception as e:
            raise ValueError(f"url: {url}, method: {method}, headers: {HEADERS}, auth: {AUTH}, json: {json}, error: {e}")
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
            description="Get a consolidated report for all DAG runs within specified time range",
            inputSchema={
                "type": "object",
                "properties": {
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
                "required": []
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
            name="list-dag-runs",
            description="Get DAG runs in batch",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of DAG IDs to query, if None get all dags from list-dag-runs tool",
                        "required": False
                    },
                    "execution_date_gte": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format, timezone is Asia/Shanghai",
                        "required": False,
                    },
                    "execution_date_lte": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format, timezone is Asia/Shanghai",
                        "required": False,
                    },
                    "states": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of states to filter",
                        "required": False
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of records to return per DAG",
                        "default": 100,
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number of records to skip",
                        "default": 0,
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Field to order by (e.g. -start_date, start_date)",
                        "default": "-start_date",
                    }
                },
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
                        "description": "Start date in YYYY-MM-DD format, timezone is Asia/Shanghai",
                        "required": False,
                    },
                    "execution_date_lte": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format, timezone is Asia/Shanghai",
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
        types.Tool(  # 新增的回填工具
            name="backfill-dag",
            description="Backfill a DAG for a specified date range",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "start_date": {"type": "string", "description": "Start date in YYYY-MM-DD format, timezone is Asia/Shanghai"},
                    "end_date": {"type": "string", "description": "End date in YYYY-MM-DD format, timezone is Asia/Shanghai"},
                },
                "required": ["dag_id", "start_date", "end_date"],
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
        start_date = arguments.get("start_date")
        days = int(arguments.get("days", 1))
        report = await get_daily_report(start_date=start_date, days=days)
        return [
            types.TextContent(
                type="text",
                text=report
            )
        ]

    if name == "list-dag-runs":
        dag_ids = arguments.get("dag_ids")
        execution_date_gte = arguments.get("execution_date_gte")
        execution_date_lte = arguments.get("execution_date_lte")
        states = arguments.get("states")
        limit = arguments.get("limit", 100)
        offset = arguments.get("offset", 0)
        order_by = arguments.get("order_by", "-start_date")

        data = await get_dag_runs_batch(
            dag_ids=dag_ids,
            execution_date_gte=execution_date_gte,
            execution_date_lte=execution_date_lte,
            states=states,
            limit=limit,
            offset=offset,
            order_by=order_by
        )

        if not data or "dag_runs" not in data:
            return [
                types.TextContent(
                    type="text",
                    text="Unable to fetch DAG runs"
                )
            ]

        result = [
            f"Total DAG Runs: {data['total_entries']}",
            "=" * 40
        ]

        for run in data["dag_runs"]:
            result.extend([
                f"\nDAG ID: {run['dag_id']}",
                f"Run ID: {run['dag_run_id']}",
                f"State: {run['state']}",
                f"Start Date: {run['start_date']}",
                f"End Date: {run['end_date'] or 'N/A'}",
                "-" * 40
            ])

        return [
            types.TextContent(
                type="text",
                text="\n".join(result)
            )
        ]

    if name == "backfill-dag":  # 新增的回填工具处理逻辑
        dag_id = arguments.get("dag_id")
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        
        result = await backfill_dag(dag_id, start_date, end_date)
        return [
            types.TextContent(
                type="text",
                text=f"""
    Successfully backfilled DAG {dag_id}
    Run ID: {log["dag_run_id"]}
    State: {log["state"]}
    """
            ) for log in result
        ]

    raise ValueError(f"Unknown tool: {name}")

async def backfill_dag(dag_id: str, start_date: str, end_date: str) -> list[dict]:
    """执行DAG的回填操作。"""
    if not dag_id or not start_date or not end_date:
        raise ValueError("Missing dag_id, start_date, or end_date")

    # 根据开始和结束日期生成日期范围数组, 兼容多种日期格式
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        date_range = [start + timedelta(days=d) for d in range((end - start).days + 1)]
    except ValueError:
        raise ValueError("Invalid date format. Please use YYYY-MM-DD format.")
    
    logs = []
    for date in date_range:
        url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns"
        param = {
            "conf": {},
            "dag_run_id": f"manual__{dag_id}_{date.isoformat()}Z",
            "logical_date": date.isoformat()+'Z'
        }
        data = await make_airflow_request(url, method="POST", json=param)
        if not data:
            data = {
                "dag_run_id": param["dag_run_id"],
                "state": "FAILED",
                "logical_date": param["logical_date"]
            }
        logs.append({
            "dag_run_id": data["dag_run_id"],
            "state": data["state"],
            "logical_date": data["logical_date"],
        })

    return logs

async def get_dag_runs_batch(
    dag_ids: list[str] | None = None,
    execution_date_gte: str | None = None,
    execution_date_lte: str | None = None,
    states: list[str] | None = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str = "-start_date"
) -> dict:
    """批量获取DAG运行状态。"""
    url = f"{AIRFLOW_API_BASE}/dags/~/dagRuns/list"

    # 构建请求体
    request_body = {
        "dag_ids": dag_ids,
        "page_offset": offset,
        "page_limit": limit,
        "order_by": order_by
    }

    if execution_date_gte:
        request_body["execution_date_gte"] = execution_date_gte
    if execution_date_lte:
        request_body["execution_date_lte"] = execution_date_lte
    if states:
        request_body["states"] = states

    data = await make_airflow_request(url, method="POST", json=request_body)
    return data

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
    start_date: str | None = None,
    days: int = 1
) -> str:
    """Get a consolidated report for all DAG runs within specified time range."""
    if days < 1 or days > 30:
        return "Days parameter must be between 1 and 30"

    if not start_date:
        start_date = datetime.utcnow().strftime("%Y-%m-%d")
        
    try:
        # 解析起始日期
        parsed_date = datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        return f"Invalid date format: {start_date}. Please use YYYY-MM-DD format."

    # 获取所有DAG列表
    dags_data = await list_dags()
    if "Unable to fetch DAGs" in dags_data:
        return "Unable to fetch DAGs list"

    dag_ids = dags_data.split("\n")

    # 初始化总体统计
    grand_total = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "running": 0,
        "other": 0,
        "failed_tasks": []
    }

    # 按天统计数据
    daily_summaries = []
    for day in range(days):
        current_date = parsed_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")
        date_start = current_date.strftime("%Y-%m-%dT00:00:00+00:00")
        date_end = current_date.strftime("%Y-%m-%dT23:59:59+00:00")

        daily_total = {
            "date": date_str,
            "total": 0,
            "success": 0,
            "failed": 0,
            "running": 0,
            "other": 0,
            "by_dag": {}
        }

        # 使用 get_dag_runs_batch 获取当天所有DAG的运行记录
        dag_run_data = await get_dag_runs_batch(
            dag_ids=dag_ids,
            execution_date_gte=date_start,
            execution_date_lte=date_end,
            states=["success", "failed", "running"],
            limit=100,
            order_by="-start_date"
        )

        # Process the DAG runs
        for run in dag_run_data.get("dag_runs", []):
            dag_id = run.get("dag_id", "unknown")
            state = run.get("state", "unknown").lower()  # Convert to lowercase for consistency
            
            # Initialize DAG stats if not exists
            if dag_id not in daily_total["by_dag"]:
                daily_total["by_dag"][dag_id] = {
                    "total": 0,
                    "success": 0,
                    "failed": 0,
                    "running": 0,
                    "queued": 0,
                    "other": 0
                }
            
            # Update DAG specific stats
            daily_total["by_dag"][dag_id]["total"] += 1
            
            # Update state counts based on the four valid states
            if state == "success":
                daily_total["by_dag"][dag_id]["success"] += 1
                daily_total["success"] += 1
                grand_total["success"] += 1
                
            elif state == "failed":
                daily_total["by_dag"][dag_id]["failed"] += 1
                daily_total["failed"] += 1
                grand_total["failed"] += 1
                
                # For failed tasks, try to get logs and analyze
                try:
                    logs = await get_dag_logs(dag_id, run.get("dag_run_id", ""))
                    analysis = analyze_logs(logs)
                    grand_total["failed_tasks"].append({
                    "date": date_str,
                    "dag_id": dag_id,
                    "run_id": run.get("dag_run_id", ""),
                    "analysis": analysis
                    })
                except Exception:
                    pass
                
            elif state == "running":
                daily_total["by_dag"][dag_id]["running"] += 1
                daily_total["running"] += 1
                grand_total["running"] += 1
                
            elif state == "queued":
                # Add handling for queued state
                if "queued" not in daily_total:
                    daily_total["queued"] = 0
                if "queued" not in grand_total:
                    grand_total["queued"] = 0
                    daily_total["by_dag"][dag_id]["queued"] += 1
                    daily_total["queued"] += 1
                    grand_total["queued"] += 1
                else:
                    daily_total["by_dag"][dag_id]["other"] += 1
                    daily_total["other"] += 1
                    grand_total["other"] += 1
            
            # Update totals
            daily_total["total"] += 1
            grand_total["total"] += 1

        daily_summaries.append(daily_total)

    # 生成报告
    result = [
        "Airflow DAGs Execution Report",
        f"Period: {start_date} to {(parsed_date + timedelta(days=days-1)).strftime('%Y-%m-%d')}",
        "=" * 50,
        "\nOverall Statistics:",
        f"计划执行任务数: {grand_total['total']}",
        f"实际执行任务数: {grand_total['total']}",  # 在这个实现中两者相同
        f"成功任务数: {grand_total['success']}",
        f"失败任务数: {grand_total['failed']}",
        f"运行中任务数: {grand_total['running']}",
        f"其他状态任务数: {grand_total['other']}",
        "\nDaily Breakdown:",
        "-" * 50
    ]

    # 添加每日统计
    for day in daily_summaries:
        result.extend([
            f"\n{day['date']}:",
            f"总计: {day['total']} | 成功: {day['success']} | 失败: {day['failed']} | 运行中: {day['running']} | 其他: {day['other']}"
        ])

        # 添加每个DAG的详细信息
        for dag_id, stats in day["by_dag"].items():
            if stats["total"] > 0:  # 只显示有运行记录的DAG
                result.append(f"  {dag_id}: 总数={stats['total']}, 成功={stats['success']}, 失败={stats['failed']}, 运行中={stats['running']}, 其他={stats['other']}")

    # # 添加失败任务分析
    # if grand_total["failed_tasks"]:
    #     result.extend([
    #         "\n失败任务分析:",
    #         "=" * 50
    #     ])

    #     for task in grand_total["failed_tasks"]:
    #         result.extend([
    #             f"\n日期: {task['date']}",
    #             f"DAG ID: {task['dag_id']}",
    #             f"运行ID: {task['run_id']}",
    #             f"错误类型: {task['analysis']['error_type']}",
    #             f"错误信息: {task['analysis']['error_message']}",
    #             "\n建议操作:",
    #             *[f"- {s}" for s in task['analysis']['suggestions']],
    #             "-" * 40
    #         ])

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