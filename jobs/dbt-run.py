import subprocess
import sys
import os

# Configuration
LOG_FILE_PATH = os.path.expandvars("$HOME/logs/dbt_structured.log")

def run_dbt_and_log(target="dev_hive"):
    # Ensure log directory exists
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

    # Inject the target parameter into the command
    command = ["dbt", "--log-format", "json", "run", "--target", target]

    print(f"--- Starting dbt execution for target: {target} ---")
    print(f"--- Command: {' '.join(command)} ---")

    try:
        # Start the process and pipe stdout
        with subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True,
            bufsize=1
        ) as proc, open(LOG_FILE_PATH, "w") as log_file:
            
            for line in proc.stdout:
                # 1. Output to stdout (Workbench Console)
                sys.stdout.write(line)
                
                # 2. Output to logfile
                log_file.write(line)
                
                # Optional: Force flush to see logs in real-time
                sys.stdout.flush()
                log_file.flush()

        # Update process exit status
        proc.wait()

        if proc.returncode != 0:
            print(f"\ndbt finished with errors (Return Code: {proc.returncode})")
        else:
            print("\ndbt finished successfully.")

    except Exception as e:
        print(f"An error occurred while running dbt: {e}")

if __name__ == "__main__":
    # You can now pull the target from an environment variable as requested
    dbt_target = os.getenv("DBT_TARGET", "dev_hive")
    run_dbt_and_log(target=dbt_target)
