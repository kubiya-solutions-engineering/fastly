import inspect

from kubiya_sdk import tool_registry
from kubiya_sdk.tools.models import Arg, Tool, FileSpec

from . import fastly_realtime

fastly_realtime_tool = Tool(
    name="fastly_realtime",
    description="As an intelligent agent named Fastly Realtime, you have the capability to interact with Fastly services.",
    type="docker",
    image="python:3.11-slim",
    args=[
        Arg(
            name="service_name",
            required=True,
            description="The name of the Fastly service to monitor"            
        ),
        Arg(
            name="environment",
            required=True,
            description="The environment to monitor (production, dev, qa)",
            ),
    ],
    secrets=["FASTLY_API_TOKEN", "SLACK_API_TOKEN"],
    env=["SLACK_CHANNEL_ID", "SLACK_THREAD_TS"],
    content="""
pip install requests slack_sdk fuzzywuzzy argparse python-Levenshtein > /dev/null 2>&1

python /tmp/fastly_realtime.py --service_name "$service_name" --environment "$environment" 
""",
    with_files=[
        FileSpec(
            destination="/tmp/fastly_realtime.py",
            content=inspect.getsource(fastly_realtime),
        ),
    ]
)


tool_registry.register("aedm", fastly_realtime_tool)
