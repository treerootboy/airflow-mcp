import asyncio
import re
from typing import Any
import httpx
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from pydantic import AnyUrl
import mcp.server.stdio

# Initialize server
server = Server("airflow")

# Constants
AIRFLOW_API_BASE = "http://localhost:8080/api/v1"
AUTH = ("admin", "admin")
HEADERS = {
    "Accept": "application/json"
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

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """List available Airflow resources."""
    dags = await list_dags()
    resources = [
        types.Resource(
            uri=AnyUrl("airflow://dags"),
            name="Airflow DAGs",
            description="List of all Airflow DAGs",
            mimeType="application/json"
        )
    ]
    
    # 动态添加每个DAG的状态资源
    for dag_id in dags.split("\n"):
        if dag_id:
            resources.extend([
                types.Resource(
                    uri=AnyUrl(f"airflow://dags/{dag_id}/status"),
                    name=f"Status of {dag_id}",
                    description=f"""Get the status of {dag_id} DAG runs.
Supports query parameters:
- limit: Maximum number of runs to return
- offset: Number of runs to skip
- order_by: Field to order by (e.g. -start_date, start_date)
- execution_date_gte: Minimum execution date
- execution_date_lte: Maximum execution date
- state: Filter by state (success, running, failed, etc.)

Example: airflow://dags/{dag_id}/status?limit=5&state=success""",
                    mimeType="application/json"
                ),
                types.Resource(
                    uri=AnyUrl(f"airflow://dags/{dag_id}/logs"),
                    name=f"Logs of {dag_id}",
                    description=f"""Get the logs of {dag_id} DAG runs.
Supports query parameters:
- run_id: (Required) The run ID to get logs for
- task_id: (Optional) Get logs for specific task, if not provided will get all tasks' logs
- try_number: (Optional) The try number of the task execution

Example: airflow://dags/{dag_id}/logs?run_id=manual__2025-03-07T18:34:52.899836+00:00""",
                    mimeType="text/plain"
                )
            ])
    
    return resources

@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    """Read a specific Airflow resource."""
    uri_str = str(uri)
    if uri_str == "airflow://dags":
        return await list_dags()
    
    status_match = re.match(r"airflow://dags/([^/]+)/status(?:\?(.*))?", uri_str)
    if status_match:
        dag_id = status_match.group(1)
        query_params = {}
        if status_match.group(2):
            params = status_match.group(2).split("&")
            for param in params:
                if "=" in param:
                    key, value = param.split("=")
                    query_params[key] = value
        
        return await get_dag_status(
            dag_id,
            limit=int(query_params.get("limit", "1")),
            offset=int(query_params.get("offset", "0")),
            order_by=query_params.get("order_by", "-start_date"),
            execution_date_gte=query_params.get("execution_date_gte"),
            execution_date_lte=query_params.get("execution_date_lte"),
            state=query_params.get("state")
        )
    
    logs_match = re.match(r"airflow://dags/([^/]+)/logs(?:\?(.*))?", uri_str)
    if logs_match:
        dag_id = logs_match.group(1)
        query_params = {}
        if logs_match.group(2):
            params = logs_match.group(2).split("&")
            for param in params:
                if "=" in param:
                    key, value = param.split("=")
                    query_params[key] = value
        
        if "run_id" not in query_params:
            raise ValueError("run_id is required for getting logs")
        
        return await get_dag_logs(
            dag_id,
            run_id=query_params["run_id"],
            task_id=query_params.get("task_id"),
            try_number=int(query_params["try_number"]) if "try_number" in query_params else None
        )
    
    raise ValueError("Resource not found")

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
            description="Get a daily report for a DAG including execution summary and failure details",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format (default: today)",
                        "required": False
                    }
                },
                "required": ["dag_id"]
            },
        )
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool execution requests."""
    if name not in ["trigger-dag", "enable-dag", "get-daily-report"]:
        raise ValueError(f"Unknown tool: {name}")

    if not arguments:
        raise ValueError("Missing arguments")

    dag_id = arguments.get("dag_id")
    if not dag_id:
        raise ValueError("Missing dag_id")

    if name == "trigger-dag":
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
    elif name == "enable-dag":
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
    elif name == "get-daily-report":
        date = arguments.get("date")  # Optional, will use today if not provided
        report = await get_daily_report(dag_id, date)
        return [
            types.TextContent(
                type="text",
                text=report
            )
        ]

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
    """Get the status of a specific DAG.
    
    Args:
        dag_id: The ID of the DAG to check
        limit: Maximum number of runs to return (default: 1)
        offset: Number of runs to skip (default: 0)
        order_by: Field to order by (default: -start_date)
        execution_date_gte: Minimum execution date
        execution_date_lte: Maximum execution date
        state: Filter by state (success, running, failed, etc.)
    """
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
    
    # 构建查询参数字符串
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
    """Get logs for a DAG run.
    
    Args:
        dag_id: The ID of the DAG
        run_id: The run ID to get logs for
        task_id: Get logs for specific task (optional)
        try_number: The try number of the task execution (optional)
    """
    # 首先获取DAG运行的所有任务
    url = f"{AIRFLOW_API_BASE}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
    data = await make_airflow_request(url)
    
    if not data or "task_instances" not in data:
        return f"Unable to fetch task instances for DAG {dag_id} run {run_id}."
    
    if not data["task_instances"]:
        return f"No task instances found for DAG {dag_id} run {run_id}."
    
    result = []
    tasks = data["task_instances"]
    
    # 如果指定了task_id，只获取该任务的日志
    if task_id:
        tasks = [task for task in tasks if task["task_id"] == task_id]
        if not tasks:
            return f"Task {task_id} not found in DAG {dag_id} run {run_id}."
    
    for task in tasks:
        task_id = task["task_id"]
        # 使用指定的try_number或者获取最后一次尝试的日志
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
    date: str | None = None
) -> str:
    """Get a daily report for a DAG including execution summary and failure details.
    
    Args:
        dag_id: The DAG ID to get report for
        date: The date to get report for (default: today)
    """
    if not date:
        from datetime import datetime
        date = datetime.utcnow().strftime("%Y-%m-%dT00:00:00+00:00")
    else:
        date = f"{date}T00:00:00+00:00"

    # 获取所有运行记录
    all_runs = await get_dag_status(
        dag_id,
        limit=100,  # 获取足够多的记录以便统计
        execution_date_gte=date,
        execution_date_lte=date.replace("00:00:00", "23:59:59")
    )

    # 获取失败的运行记录
    failed_runs = await get_dag_status(
        dag_id,
        limit=100,
        state="failed",
        execution_date_gte=date,
        execution_date_lte=date.replace("00:00:00", "23:59:59")
    )

    result = [
        f"Daily Report for DAG: {dag_id}",
        f"Date: {date[:10]}",
        "=" * 50,
        "Execution Summary:",
        all_runs,
        "-" * 50,
    ]

    if "No runs found" not in failed_runs:
        result.extend([
            "Failed Runs Details:",
            failed_runs,
            "=" * 50,
        ])

        # 获取每个失败运行的日志
        failed_run_ids = []
        for line in failed_runs.split("\n"):
            if line.startswith("Run ID: "):
                failed_run_ids.append(line.replace("Run ID: ", "").strip())

        for run_id in failed_run_ids:
            logs = await get_dag_logs(dag_id, run_id)
            result.extend([
                f"Logs for failed run {run_id}:",
                logs,
                "=" * 50
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