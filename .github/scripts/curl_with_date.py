import os
import sys
import datetime
import pytz
import subprocess
from datetime import timedelta

LOG_FILE = os.path.join(os.path.dirname(__file__), 'curl_execution.log')

def log_message(message):
    """Log messages to both console and log file"""
    timestamp = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = f"[{timestamp}] {message}\n"
    
    print(log_entry.strip())
    
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)

def increment_date(date_str):
    """Increment date in YYYYMMDD format by one day"""
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    next_date = date + timedelta(days=1)
    return next_date.strftime("%Y%m%d")

def get_current_date():
    """Get current date in YYYYMMDD format"""
    return datetime.datetime.now(pytz.utc).strftime("%Y%m%d")

def generate_curl_command(current_date, ctk_cookie):
    """Generate the curl command with the current date"""
    job_name = f"ene-ats-integration-batch-prod-uber-stage5362df53-ad63-4c7a-9be1-f2b33fc83e74_{current_date}"
    id_value = f"funneltracking/bullhorn/adecco/GeneralStaffing/BH_AGS_JOB_SUBMISSION_JOVEO_OUT_{current_date}.csv$5362df53-ad63-4c7a-9be1-f2b33fc83e74,78a64614-9ca3-4b42-824c-4aac37237984,51a55bc3-c156-4c8f-9f6e-1bbe7f496d0f"
    
    curl_command = [
        'curl',
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
    
    return curl_command

def main():
    log_message("Script execution started")
    
    if len(sys.argv) < 4:
        log_message("Error: Missing arguments. Usage: python curl_with_date.py <start_date> <end_date> <ctk_cookie>")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    ctk_cookie = sys.argv[3]
    
    log_message(f"Received parameters - Start Date: {start_date}, End Date: {end_date}")
    
    # Get the current date (UTC)
    current_date = get_current_date()
    log_message(f"Current system date (UTC): {current_date}")
    
    # Check if current date is within range
    if start_date <= current_date <= end_date:
        log_message(f"Current date is within range. Preparing cURL command for date: {current_date}")
        
        curl_cmd = generate_curl_command(current_date, ctk_cookie)
        
        log_message("Generated cURL command:")
        log_message(" ".join(curl_cmd))
        
        # Execute the curl command
        try:
            log_message("Executing cURL command...")
            result = subprocess.run(curl_cmd, check=True, capture_output=True, text=True)
            
            log_message("cURL command executed successfully")
            log_message(f"Response: {result.stdout}")
            
            # Update the last successful run date
            with open(LOG_FILE, 'a') as f:
                f.write(f"\nLast successful execution for date: {current_date}\n")
                f.write(f"Command: {' '.join(curl_cmd)}\n")
                f.write(f"Response: {result.stdout}\n")
            
        except subprocess.CalledProcessError as e:
            log_message(f"Error executing cURL command: {e.stderr}")
            sys.exit(1)
    else:
        log_message(f"Current date {current_date} is outside the specified range ({start_date} to {end_date}). No action taken.")
        sys.exit(0)

if __name__ == "__main__":
    main()