#!/usr/bin/env python3
"""
Process monitor for CSV to Iceberg conversion.

This script monitors the stdout of a CSV to Iceberg conversion process,
extracts progress information, and reports it to the web application through API calls.
"""
import sys
import re
import time
import argparse
import subprocess
import logging
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('process_monitor')

def extract_progress(line):
    """Extract progress percentage from a log line."""
    # First pattern: Look for standard Progress: lines from logger
    pattern1 = r'Progress:\s*(\d+)%'
    match = re.search(pattern1, line)
    if match:
        return int(match.group(1))
    
    # Second pattern: Look for Rich console fancy formatted lines
    pattern2 = r'Writing data:\s*(\d+)%\s*complete'
    match = re.search(pattern2, line)
    if match:
        return int(match.group(1))
    
    # Third pattern: Look for any numbers followed by % (more liberal matching)
    pattern3 = r'(\d+)%'
    match = re.search(pattern3, line)
    if match:
        return int(match.group(1))
    
    return None

def report_progress(job_id, percent, api_url):
    """Report progress to the web application API."""
    try:
        url = f"{api_url}/job/{job_id}/progress/{percent}"
        response = requests.post(url)
        if response.status_code == 200:
            logger.debug(f"Progress ({percent}%) reported successfully for job {job_id}")
        else:
            logger.error(f"Failed to report progress for job {job_id}: {response.status_code}")
    except Exception as e:
        logger.error(f"Error reporting progress for job {job_id}: {str(e)}")

def monitor_process(job_id, cmd, api_url="http://localhost:5000"):
    """
    Run a subprocess and monitor its output for progress information.
    
    Args:
        job_id: Job ID to report progress for
        cmd: Command to run as a list of strings
        api_url: Base URL of the web application API
    """
    try:
        logger.info(f"Starting process for job {job_id} with command: {' '.join(cmd)}")
        
        # Start the process
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Track progress
        last_progress = 0
        last_report_time = time.time()
        progress_reported = False
        
        # Report initial progress (0%)
        report_progress(job_id, 0, api_url)
        
        # Process each line of output
        for line in iter(process.stdout.readline, ''):
            # Print to stdout for debugging
            print(line, end='')
            
            # Check for progress information
            progress = extract_progress(line)
            if progress is not None:
                # Only report if progress has increased or if it's been more than 5 seconds since last report
                current_time = time.time()
                time_since_last_report = current_time - last_report_time
                
                if progress > last_progress or time_since_last_report > 5:
                    logger.info(f"Detected progress: {progress}%, reporting to API")
                    last_progress = progress
                    last_report_time = current_time
                    report_progress(job_id, progress, api_url)
                    progress_reported = True
                
        # Wait for the process to complete
        process.stdout.close()
        return_code = process.wait()
        
        # Report 100% progress if completed successfully or at least 90% progress was reached
        if return_code == 0:
            if last_progress < 100:
                logger.info("Process completed successfully, reporting 100% completion")
                report_progress(job_id, 100, api_url)
            else:
                logger.info("Process already reported 100% completion")
        else:
            # For errors, make sure we report up to the progress we reached
            if progress_reported:
                logger.info(f"Process failed but reporting last progress: {last_progress}%")
                # We don't reset progress on failure
            else:
                logger.info("Process failed without reporting any progress")
        
        logger.info(f"Process completed with return code {return_code}")
        return return_code
        
    except Exception as e:
        logger.error(f"Error monitoring process: {str(e)}")
        return 1

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Monitor CSV to Iceberg conversion progress")
    parser.add_argument("--job-id", required=True, help="Conversion job ID")
    parser.add_argument("--api-url", default="http://localhost:5000", help="Base URL of the web application API")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command to run and monitor")
    
    args = parser.parse_args()
    
    if not args.command:
        logger.error("No command specified")
        return 1
        
    # Remove the -- separator if present
    cmd = args.command
    if cmd[0] == "--":
        cmd = cmd[1:]
        
    return monitor_process(args.job_id, cmd, args.api_url)

if __name__ == "__main__":
    sys.exit(main())