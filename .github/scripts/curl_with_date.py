import os
import sys
import json
import datetime
from datetime import datetime, timedelta
import subprocess

# File paths
STATE_FILE = os.path.join(os.path.dirname(__file__), 'state.json')
LOG_FILE = os.path.join(os.path.dirname(__file__), 'processing_log.txt')

def validate_date(date_str):
    """Validate YYYYMMDD date format"""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        return False

def log(message):
    """Log messages with timestamp"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(log_entry + "\n")
    except IOError as e:
        print(f"Failed to write to log file: {e}")

def load_state(start_date, end_date):
    """Load or initialize processing state"""
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        log(f"Loaded existing state: {json.dumps(state, indent=2)}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log(f"Initializing new state (reason: {str(e)})")
        state = {
            "current_date": start_date,
            "start_date": start_date,
            "end_date": end_date,
            "processed_dates": [],
            "last_run": None
        }
    return state

def save_state(state):
    """Save processing state"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except IOError as e:
        log(f"Failed to save state: {e}")

def date_range(start, end):
    """Generate dates from start to end (YYYYMMDD format)"""
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    current_dt = start_dt
    
    while current_dt <= end_dt:
        yield current_dt.strftime("%Y%m%d")
        current_dt += timedelta(days=1)

def process_date(date, ctk_cookie):
    """Execute the curl command for a specific date"""
    job_name = f"ene-ats-integration-batch-prod-uber-stage5362df53-ad63-4c7a-9be1-f2b33fc83e74_{date}"
    id_value = f"funneltracking/bullhorn/adecco/GeneralStaffing/BH_AGS_JOB_SUBMISSION_JOVEO_OUT_{date}.csv$5362df53-ad63-4c7a-9be1-f2b33fc83e74,78a64614-9ca3-4b42-824c-4aac37237984,51a55bc3-c156-4c8f-9f6e-1bbe7f496d0f"
    
    curl_cmd = [
        'curl',
        '--max-time', '30',  # 30 second timeout
        '--location',
        'ene-apply-batch-orchestrator.prod.joveo.com/api/trigger',
        '--header',
        'Content-Type: application/json',
        '--header',
        f'Cookie: CTK={ctk_cookie}',
        '--data',
        f'''{{
            "jobName" : "{job_name}",
            "jobQueue" : "ene-ats-integration-batch-prod-uber-stage",
            "jobDefinition" : "ene-ats-integration-batch-prod",
            "envVars": {{
                "spring.config.import": "configserver:http://spring-config-server.prod.joveo.com:8888/",
                "SPRING_CONFIG_SERVER_URL": "spring-config-server.prod.joveo.com",
                "ENVIRONMENT": "production",
                "name": "ADECCO_BULLHORN_APPLY_EVENT_SYNCER",
                "JOVEO_ENV": "production",
                "id": "{id_value}",
                "SPRING_CONFIG_SERVER_PORT": "8888",
                "receiptHandle":"test"
            }}
        }}'''
    ]
    
    log(f"Executing for date: {date}")
    log(f"Command: {' '.join(curl_cmd)}")
    
    try:
        result = subprocess.run(curl_cmd, check=True, capture_output=True, text=True)
        log(f"Success: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        log(f"Error: {e.stderr}")
        return False

def main():
    if len(sys.argv) < 4:
        log("Error: Missing arguments. Usage: python date_processor.py <start_date> <end_date> <ctk_cookie>")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    ctk_cookie = sys.argv[3]
    
    if not all(validate_date(d) for d in [start_date, end_date]):
        log("Error: Dates must be in YYYYMMDD format")
        sys.exit(1)
    
    state = load_state(start_date, end_date)
    state['last_run'] = datetime.utcnow().isoformat()
    
    # Process the next date in sequence
    for date in date_range(state['current_date'], end_date):
        success = process_date(date, ctk_cookie)
        if success:
            state['processed_dates'].append(date)
            state['current_date'] = (datetime.strptime(date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            save_state(state)
            log(f"Successfully processed date {date}. Updated state.")
            break
        else:
            log(f"Failed to process date {date}. Will retry next run.")
            break
    
    save_state(state)
    log("Processing complete for this run.")

if __name__ == "__main__":
    main()