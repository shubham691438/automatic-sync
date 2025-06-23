import os
import sys
import datetime
import pytz
import subprocess

def increment_date(date_str):
    """Increment date in YYYYMMDD format by one day"""
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    next_date = date + datetime.datetime.timedelta(days=1)
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
    if len(sys.argv) < 4:
        print("Usage: python curl_with_date.py <start_date> <end_date> <ctk_cookie>")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    ctk_cookie = sys.argv[3]
    
    # Get the current date (UTC)
    current_date = get_current_date()
    
    # Check if current date is within range
    if start_date <= current_date <= end_date:
        curl_cmd = generate_curl_command(current_date, ctk_cookie)
        
        # Print the command for debugging
        print("Executing command:")
        print(" ".join(curl_cmd))
        
        # Execute the curl command
        try:
            result = subprocess.run(curl_cmd, check=True, capture_output=True, text=True)
            print("cURL command executed successfully:")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error executing cURL command:")
            print(e.stderr)
            sys.exit(1)
    else:
        print(f"Current date {current_date} is outside the specified range ({start_date} to {end_date}).")
        sys.exit(0)

if __name__ == "__main__":
    main()