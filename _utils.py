import asyncio
import sys
from pathlib import Path
from types import coroutine
from typing import Any

from cams_ncp_client.client import CamsNcpApiClient
from prefect import State
from prefect.blocks.system import Secret
from prefect.client.schemas.filters import FlowFilterName, FlowFilter, FlowRunFilter, FlowFilterId, FlowRunFilterId
from prefect.variables import Variable
from vito.sas.air.cams_client import CAMSEuropeClient, Pollutant
from prefect.client import get_client
from prefect.utilities.asyncutils import sync_compatible
from datetime import datetime, timedelta, timezone

_ncp_api_client: CamsNcpApiClient | None  = None

@sync_compatible
async def assert_recent_flow_run(flow_name: str, hours: int = 8) -> None:
    """
    Assert that a flow has been run successfully within the last `hours` hours.
    """
    flow_ok = await was_flow_successful_recently(flow_name, hours=8)
    print(f"Flow '{flow_name}' was run successfully in the last {hours} hours: {flow_ok}")
    if flow_ok:
        print(f"Flow '{flow_name}' ran successfully in the last {hours} hours.")
        return

    print(f"Flow '{flow_name}' has not run successfully in the last {hours} hours. Triggering it now...")

    async with get_client() as client:
        flow = await client.read_flow_by_name(flow_name=flow_name)
        if not flow:
            raise ValueError(f"Flow '{flow_name}' not found.")

        deployments = await client.read_deployments(flow_filter=FlowFilter(id=FlowFilterId(any_=[flow.id])))
        if not deployments:
            raise ValueError(f"No deployments found for flow '{flow_name}'.")

        deployment = deployments[0]  # optionally select based on name or tag
        flow_run = await client.create_flow_run_from_deployment(
            deployment_id=deployment.id,
            name=f"{flow_name}-manual-run-from-assert_recent_flow_run()-at-{datetime.now().isoformat()}",
            parameters={}  # Optional: set flow parameters here
        )

        # Poll the flow run status until completion
        while True:
            run = await client.read_flow_run(flow_run.id)
            state: State = run.state
            if state.is_final():
                if state.name == "Completed":
                    print(f"Flow '{flow_name}' completed successfully.")
                else:
                    raise RuntimeError(f"Flow '{flow_name}' did not complete successfully. Final state: {state.name}")
                break
            await asyncio.sleep(10)  # Wait before checking again


@sync_compatible
async def was_flow_successful_recently(flow_name: str, hours: int = 8) -> bool:
    async with get_client() as client:
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=hours)

        # Get flow by name
        # print all flow names to debug this code
        print(f"Checking for flow: {flow_name}")
        print("Available flows:")
        all_flows = await client.read_flows()
        for flow in all_flows:
            print(f"- {flow.name} (ID: {flow.id})")

        # flow for name
        flow_name_filter = FlowFilter(name=FlowFilterName(any_=[flow_name]))
        flows = await client.read_flows(flow_filter=flow_name_filter)

        print(f"flows for name {flow_name}: ", flows)
        if not flows:
            return False

        flow_id = flows[0].id

        print(f"flow_id: ", flow_id)

        runs = await client.read_flow_runs(flow_filter=flow_name_filter)
        print(f"runs for flow {flow_name}: ", len(runs))

        for run in runs:
            if run.state.name == "Completed" and run.end_time and run.end_time >= since:
                return True

        return False

def ncp_api_client() -> CamsNcpApiClient:
    global _ncp_api_client
    if _ncp_api_client is None:
        api_base_url = Variable.get("cams_ncp_api_base_url", "http://127.0.0.1:5050")
        # api_base_url = str(_get_var("cams_ncp_api_base_url", default=None))

        print(f"Using api_base_url {api_base_url}")
        _ncp_api_client = CamsNcpApiClient(api_base_url)
    return _ncp_api_client


def get_secret(var_name: str) -> str:
    # Secret(value="secret!-1234567890").save(var_name, overwrite=True)  # to overwrite a Secret
    secret_block = Secret.load(var_name)
    api_key = secret_block.get()
    if api_key is None or api_key.strip() == "":
        raise ValueError(f"Sectret {var_name} is not set. Please set the secret in Prefect.")
    return api_key


def print_env():
    # print current Python executable
    print(f"Python executable: {sys.executable}")
    # print the current WD and the content of the current directory
    print(f"Current working directory: {Path.cwd()}")
    print(f"Content of current directory: {list(Path.cwd().iterdir())}")

    # print the environment variables
    print("Environment variables:")
    for key, value in sorted(sys.modules['os'].environ.items()):
        print(f"{key}: {value}")

    # print the sys path
    print("Python sys.path:")
    for path in sys.path:
        print(path)
