import os
import sys
import json
import datetime
from datetime import datetime, timedelta
import subprocess
import traceback

# File paths
STATE_FILE = os.path.join(os.path.dirname(__file__), 'state.json')
LOG_FILE = os.path.join(os.path.dirname(__file__), 'processing_log.txt')
ERROR_LOG_FILE = os.path.join(os.path.dirname(__file__), 'error_log.txt')

def validate_date(date_str):
    """Validate YYYYMMDD date format"""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        return False

def log(message, level="INFO"):
    """Enhanced logging with different levels"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = f"[{timestamp}] [{level}] {message}"
    print(log_entry)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(log_entry + "\n")
    except IOError as e:
        print(f"Failed to write to log file: {e}")

def log_error(error_details):
    """Special error logging with stack trace"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with open(ERROR_LOG_FILE, 'a') as f:
            f.write(f"\n[ERROR {timestamp}]\n{error_details}\n")
    except IOError as e:
        print(f"Failed to write to error log: {e}")

def load_state(start_date, end_date):
    """Load or initialize processing state with validation"""
    default_state = {
        "current_date": start_date,
        "start_date": start_date,
        "end_date": end_date,
        "processed_dates": [],
        "failed_attempts": {},
        "last_run": None,
        "consecutive_failures": 0
    }
    
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        # Validate loaded state
        if not all(key in state for key in default_state):
            raise ValueError("Invalid state structure")
            
        log(f"Loaded existing state: {json.dumps(state, indent=2)}", "DEBUG")
        return state
        
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        log(f"Initializing new state (reason: {str(e)})", "WARNING")
        return default_state

def save_state(state):
    """Save processing state with error handling"""
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except IOError as e:
        log(f"Failed to save state: {e}", "ERROR")

def date_range(start, end):
    """Generate dates from start to end with validation"""
    try:
        start_dt = datetime.strptime(start, "%Y%m%d")
        end_dt = datetime.strptime(end, "%Y%m%d")
        
        if start_dt > end_dt:
            raise ValueError("Start date cannot be after end date")
            
        current_dt = start_dt
        while current_dt <= end_dt:
            yield current_dt.strftime("%Y%m%d")
            current_dt += timedelta(days=1)
            
    except ValueError as e:
        log(f"Invalid date range: {e}", "ERROR")
        raise

def process_date(date, ctk_cookie):
    """Execute the curl command with comprehensive error handling"""
    job_name = f"ene-ats-integration-batch-prod-uber-stage5362df53-ad63-4c7a-9be1-f2b33fc83e74_{date}"
    id_value = f"funneltracking/bullhorn/adecco/GeneralStaffing/BH_AGS_JOB_SUBMISSION_JOVEO_OUT_{date}.csv$5362df53-ad63-4c7a-9be1-f2b33fc83e74,78a64614-9ca3-4b42-824c-4aac37237984,51a55bc3-c156-4c8f-9f6e-1bbe7f496d0f"
    
    curl_cmd = [
    'curl',
        '-v',  
        '--max-time', '120',
        '--connect-timeout', '30',
        '--retry', '3',
        '--retry-delay', '5',
        '--location',
        'https://ene-apply-batch-orchestrator.prod.joveo.com/api/trigger',
        '--header', 'Content-Type: application/json',
        '--header', f'Cookie: {ctk_cookie}',
        '--data',
        json.dumps({
            "jobName": job_name,
            "jobQueue": "ene-ats-integration-batch-prod-uber-stage",
            "jobDefinition": "ene-ats-integration-batch-prod",
            "envVars": {
                "spring.config.import": "configserver:http://spring-config-server.prod.joveo.com:8888/",
                "SPRING_CONFIG_SERVER_URL": "spring-config-server.prod.joveo.com",
                "ENVIRONMENT": "production",
                "name": "ADECCO_BULLHORN_APPLY_EVENT_SYNCER",
                "JOVEO_ENV": "production",
                "id": id_value,
                "SPRING_CONFIG_SERVER_PORT": "8888",
                "receiptHandle": "test"
            }
        })
    ]

    
    log(f"Attempting to process date: {date}", "INFO")
    log(f"API Command: {' '.join(curl_cmd)}", "DEBUG")
    
    try:
        result = subprocess.run(
            curl_cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=45  # Overall timeout
        )
        
        response = {
            "success": True,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
            "output": result.stdout,
            "error": None
        }
        
        log(f"API call successful for date {date}. Response: {result.stdout[:200]}...", "INFO")
        return response
        
    except subprocess.CalledProcessError as e:
        error_response = {
            "success": False,
            "status": "API_ERROR",
            "timestamp": datetime.utcnow().isoformat(),
            "output": e.stdout,
            "error": e.stderr,
            "returncode": e.returncode
        }
        
        log(f"API call failed for date {date}. Error: {e.stderr}", "ERROR")
        log_error(json.dumps(error_response, indent=2))
        return error_response
        
    except Exception as e:
        error_response = {
            "success": False,
            "status": "UNKNOWN_ERROR",
            "timestamp": datetime.utcnow().isoformat(),
            "output": None,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        
        log(f"Unexpected error processing date {date}: {str(e)}", "CRITICAL")
        log_error(json.dumps(error_response, indent=2))
        return error_response

def main():
    try:
        if len(sys.argv) < 4:
            log("Error: Missing arguments. Usage: python date_processor.py <start_date> <end_date> <ctk_cookie>", "ERROR")
            sys.exit(1)
        
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        ctk_cookie = sys.argv[3]
        
        if not all(validate_date(d) for d in [start_date, end_date]):
            log("Error: Dates must be in YYYYMMDD format", "ERROR")
            sys.exit(1)
        
        state = load_state(start_date, end_date)
        state['last_run'] = datetime.utcnow().isoformat()
        
        # Process the next date in sequence
        for date in date_range(state['current_date'], end_date):
            result = process_date(date, ctk_cookie)
            
            if result['success']:
                state['processed_dates'].append(date)
                state['current_date'] = (datetime.strptime(date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                state['consecutive_failures'] = 0
                
                # Log full success details
                log(f"Successfully processed date {date}. Full response: {json.dumps(result, indent=2)}", "DEBUG")
            else:
                state.setdefault('failed_attempts', {})
                state['failed_attempts'][date] = state['failed_attempts'].get(date, 0) + 1
                state['consecutive_failures'] += 1
                
                # Critical failure handling
                if state['consecutive_failures'] >= 3:
                    log("Aborting: 3 consecutive failures reached", "CRITICAL")
                    break
                
                log(f"Failed to process date {date}. Will retry. Failure count: {state['failed_attempts'][date]}", "WARNING")
            
            save_state(state)
            break  # Process one date per run
        
        save_state(state)
        log("Processing complete for this run.", "INFO")
        
    except Exception as e:
        log(f"Fatal error in main execution: {str(e)}\n{traceback.format_exc()}", "CRITICAL")
        log_error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()