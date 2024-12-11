#!/usr/bin/env python3

import os
import requests
import json
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
from pprint import pprint
import time
import re
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import argparse

VALID_ENVIRONMENTS = ['production', 'dev', 'qa']
API_TOKEN = os.getenv("FASTLY_API_TOKEN")
SLACK_API_TOKEN = os.getenv("SLACK_API_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
SLACK_THREAD_TS = os.getenv("SLACK_THREAD_TS")
CACHE_FILE = "services_cache.json"
FIELDS_CACHE_FILE = "fields_cache.json"
CACHE_EXPIRY_HOURS = 24
TIME_UNITS = ['second', 'seconds', 'minute', 'minutes', 'hour', 'hours', 'day', 'days', 'week', 'weeks', 'month', 'months']
FUZZY_MATCH_THRESHOLD = 80
REAL_TIME_BASE_URL = "https://rt.fastly.com"
HISTORICAL_BASE_URL = "https://api.fastly.com"
DEFAULT_STREAM_DURATION = 60
DEFAULT_WAIT_INTERVAL = 1
FASTLY_DASHBOARD_REALTIME_URL = "https://manage.fastly.com/observability/dashboard/system/overview/realtime/{service_id}?range={range}"

COMMON_FIELDS = ["status_5xx", "requests", "hits", "miss", "all_pass_requests"]

def debug_print(message):
    if os.getenv("KUBIYA_DEBUG"):
        print(message)

def load_cache(cache_file):
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                cache_timestamp = datetime.fromisoformat(cache_data['timestamp'])
                if datetime.utcnow() - cache_timestamp < timedelta(hours=CACHE_EXPIRY_HOURS):
                    return cache_data['data']
    except Exception as e:
        print(f"Error loading cache from {cache_file}: {e}")
    return None

def save_cache(cache_file, data):
    try:
        cache_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'data': data
        }
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f)
    except Exception as e:
        print(f"Error saving cache to {cache_file}: {e}")

def list_services():
    cached_services = load_cache(CACHE_FILE)
    if cached_services:
        debug_print("Loaded services from cache.")
        return cached_services

    url = f"{HISTORICAL_BASE_URL}/service"
    headers = {
        "Fastly-Key": API_TOKEN,
        "Accept": "application/json"
    }
    params = {
        "direction": "ascend",
        "page": 1,
        "per_page": 20,
        "sort": "created"
    }
    
    all_services = {}
    
    try:
        while True:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            services = response.json()
            if not services:
                break
            for service in services:
                all_services[service['name']] = service['id']
            params["page"] += 1
    except requests.exceptions.RequestException as e:
        print(f"Error fetching services from Fastly API: {e}")
    
    save_cache(CACHE_FILE, all_services)
    return all_services

def construct_service_prefix(service_name, environment):
    if environment == 'production':
        return service_name
    return f"{environment}.{service_name.replace(' ', '')}"

def get_environment(env_name):
    if not env_name:
        return None
    env_name = env_name.lower()
    if env_name in VALID_ENVIRONMENTS:
        return env_name
    return None

def get_real_time_data(api_token, service_id, duration_seconds=5):
    url = f"{REAL_TIME_BASE_URL}/v1/channel/{service_id}/ts/0"
    debug_print(f"Real-Time API URL: {url}")
    headers = {
        "Fastly-Key": api_token,
        "Accept": "application/json"
    }
    
    try:
        debug_print("Retrieving real-time data...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        real_time_data = response.json()
        return real_time_data['Data']
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving real-time data from Fastly API: {e}")
        return None

# def get_best_match(prefix, services):
#     results = process.extract(prefix, services, scorer=fuzz.WRatio)
#     filtered_results = [result for result in results if result[0].startswith(prefix)]
#     if not filtered_results:
#         best_match = max(results, key=lambda x: x[1])
#     else:
#         best_match = max(filtered_results, key=lambda x: x[1])
#     return best_match[0] if best_match else None

# def filter_services_by_environment(environment, services):
#     # Filter services to those that start with the specified environment
#     environment_prefix = f"{environment}."
#     environment_hyphen = f"{environment}-"
#     return {name: service_id for name, service_id in services.items() if name.startswith(environment_prefix) or name.startswith(environment_hyphen)}

def filter_services_by_environment(environment, services):
    if environment == 'production':
        # For production, return services that don't start with 'dev.' or 'qa.'
        return {name: service_id for name, service_id in services.items() 
                if not name.startswith('dev.') and not name.startswith('qa.')}
    else:
        # For dev and qa, keep the existing logic
        environment_prefix = f"{environment}."
        environment_hyphen = f"{environment}-"
        return {name: service_id for name, service_id in services.items() 
                if name.startswith(environment_prefix) or name.startswith(environment_hyphen)}

# def get_best_match(service_name, filtered_services):
#     # Perform fuzzy matching on the filtered list of services
#     results = process.extract(service_name, filtered_services.keys(), scorer=fuzz.WRatio)
    
#     if not results:
#         raise ValueError(f"No services found that match '{service_name}'.")
    
#     # Return the best match from the filtered results
#     best_match = max(results, key=lambda x: x[1])[0]
#     return best_match

def get_best_match(service_name, filtered_services, environment):
    if environment == 'production':
        # For production, look for an exact match first
        for name in filtered_services.keys():
            if name.startswith(service_name + '.'):
                return name

    # If no exact match found for production or for other environments, perform fuzzy matching
    results = process.extract(service_name, filtered_services.keys(), scorer=fuzz.WRatio)
    
    if not results:
        raise ValueError(f"No services found that match '{service_name}' in the '{environment}' environment.")
    
    # Return the best match from the filtered results
    best_match = max(results, key=lambda x: x[1])[0]
    return best_match

def format_value(value):
    try:
        value = float(value)  # Ensure the value is a number
        if value >= 1000:
            return f"{value / 1000:.1f}K ({int(value)})"
        return str(int(value))
    except (ValueError, TypeError):
        return str(value)

def send_slack_message(channel, thread_ts, blocks, text="Message from script"):
    client = WebClient(token=SLACK_API_TOKEN)
    try:
        response = client.chat_postMessage(channel=channel, thread_ts=thread_ts, blocks=blocks, text=text)
        return response["channel"], response["ts"]
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")
        return None

def update_slack_message(channel, ts, blocks, text="Updated message from script", thread_ts=None):
    client = WebClient(token=SLACK_API_TOKEN)
    try:
        if thread_ts:
            client.chat_update(channel=channel, ts=ts, thread_ts=thread_ts, blocks=blocks, text=text)
        else:
            client.chat_update(channel=channel, ts=ts, blocks=blocks, text=text)
    except SlackApiError as e:
        print(f"Error updating message on Slack: {e.response['error']}")

def generate_dashboard_url(service_id, range_str):
    return FASTLY_DASHBOARD_REALTIME_URL.format(service_id=service_id, range=range_str)

def generate_slack_blocks(summary, interval_summary, service_name, environment, service_id, previous_interval_summary=None):
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":bar_chart: Real-Time Data Summary"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Service Name:*\n<{generate_dashboard_url(service_id, '1m')}|{service_name}>"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Environment:*\n{environment.title()}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Update Frequency:*\nEvery 1 second"
                }
            ]
        },
        {"type": "divider"}
    ]

    for field, value in summary.items():
        interval_value = interval_summary.get(field, 0)
        previous_value = previous_interval_summary.get(field, 0) if previous_interval_summary else 0
        change_emoji = ""
        if interval_value > previous_value:
            change_emoji = " :arrow_up:"
        elif interval_value < previous_value:
            change_emoji = " :small_red_triangle_down:"

        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*{field.replace('_', ' ').title()}*\n*Last Interval:* `{format_value(interval_value)}` {change_emoji}"
                }
            ]
        })

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "_You can stop the stream by clicking on the 'Stop' button on this thread._"
        }
    })

    return blocks

def generate_final_slack_blocks_with_intervals(summary, interval_summary, service_name, environment, service_id):
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":bar_chart: Final Real-Time Data Summary"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Service Name:*\n<{generate_dashboard_url(service_id, '1m')}|{service_name}>"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Environment:*\n{environment.title()}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Update Frequency:*\nEvery 1 second"
                }
            ]
        },
        {"type": "divider"}
    ]

    for field, value in summary.items():
        interval_value = interval_summary.get(field, 0)
        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*{field.replace('_', ' ').title()}*\n*Last Interval:* `{format_value(interval_value)}`"
                }
            ]
        })

    return blocks

def stream_real_time_data(api_token, service_name, environment, service_id, duration, wait_interval, slack_channel, thread_ts):
    print(f"Streaming real-time data for {duration} seconds with a wait interval of {wait_interval} seconds...")
    end_time = datetime.utcnow() + timedelta(seconds=duration)
    total_stats = {field: 0 for field in COMMON_FIELDS}
    previous_stats = {field: 0 for field in COMMON_FIELDS}

    slack_ts = None
    if slack_channel:
        blocks = generate_slack_blocks(total_stats, {}, service_name, environment, service_id)
        channel, slack_ts = send_slack_message(slack_channel, thread_ts, blocks)
    
    try:
        while datetime.utcnow() < end_time:
            time.sleep(wait_interval)
            stats_data = get_real_time_data(api_token, service_id, duration_seconds=wait_interval)
            if not stats_data:
                print("Unable to retrieve real-time data.")
                return

            interval_stats = {field: 0 for field in COMMON_FIELDS}
            for data_point in stats_data:
                for common_field in COMMON_FIELDS:
                    if common_field in data_point['aggregated']:
                        interval_stats[common_field] += data_point['aggregated'][common_field]

            for field in COMMON_FIELDS:
                total_stats[field] += interval_stats[field]

            if slack_channel:
                blocks = generate_slack_blocks(total_stats, interval_stats, service_name, environment, service_id, previous_interval_summary=previous_stats)
                update_slack_message(channel, slack_ts, blocks, thread_ts=thread_ts)
                previous_stats = interval_stats.copy()
            else:
                print(f"\nReal-Time Data Summary (Last {wait_interval} seconds):")
                for field, value in interval_stats.items():
                    print(f"{field}: {format_value(value)}")
                print("\n---\n")

        if not slack_channel:
            print("\nTotal Real-Time Data Summary:")
            for field, value in total_stats.items():
                print(f"{field}: {format_value(value)}")
            print("\n---\n")
    finally:
        if slack_channel and slack_ts:
            final_blocks = generate_final_slack_blocks_with_intervals(total_stats, previous_stats, service_name, environment, service_id)
            update_slack_message(slack_channel, slack_ts, final_blocks, thread_ts=thread_ts)

def main(environment, service_name):
    try:
        environment = get_environment(environment)
        if not environment:
            print(f"No matching environment found for '{environment}'. Available environments: {VALID_ENVIRONMENTS}")
            return

        debug_print("Fetching list of services...")
        services = list_services()
        
        if not services:
            print("No services found.")
            return

        # Filter services by the specified environment
        filtered_services = filter_services_by_environment(environment, services)
        
        # Get the best match within the filtered services
        best_match = get_best_match(service_name, filtered_services, environment)
        if not best_match:
            print(f"No matching service found for '{service_name}' in the '{environment}' environment.")
            return

        service_id = services[best_match]
        debug_print(f"Best matching service: {best_match}")

        stream_real_time_data(API_TOKEN, best_match, environment, service_id, DEFAULT_STREAM_DURATION, DEFAULT_WAIT_INTERVAL, SLACK_CHANNEL_ID, SLACK_THREAD_TS)
        print(f"View more details in the Fastly dashboard: {generate_dashboard_url(service_id, f'{DEFAULT_STREAM_DURATION}s')}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieve Fastly service data.")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--service_name", required=True)

    args = parser.parse_args()
    main(args.environment, args.service_name)