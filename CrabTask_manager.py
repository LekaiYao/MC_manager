import os
import subprocess
import datetime
import re

CYAN = "\033[96m"
ORANGE = "\033[38;5;214m"
RED = "\033[91m"
GREEN = "\033[92m"
GREY = "\033[90m"
RESET = "\033[0m"

def parse_jobs_count(s):
    if s is None:
        return 0
    s = s.strip()
    if '/' in s:
        s = s.split('/')[0]
    m = re.search(r'(\d+)', s)
    return int(m.group(1)) if m else 0

def manage_crab_tasks(keyword=None):
    # Define the CRAB projects directory and log file
    RE_SUBMIT = False  # Set to True to enable automatic resubmission of failed jobs
    STEP_DIR_MAP = {
    "GEN": "../CMSSW_10_6_20_patch1/src/crab_projects",
    "SIM": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "DIGI": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "HLT": "../CMSSW_10_2_16_UL/src/crab_projects",
    "RECO": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "MINIAOD": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "NTUPLE":"../CMSSW_10_6_20/src/NtupleMaker/NtupleMaker/test/crab_projects",
    }
    crab_jobs_dir = STEP_DIR_MAP[keyword]
    log_filename = f"./log/CrabTask_manager_jobStatus_{keyword}.log"
    log_OUTPUT_filename = f"./txt/CrabTask_manager_OUTPUT_DIRs_{keyword}.txt"
    output_datasets = []
    os.makedirs("./log", exist_ok=True)
    os.makedirs("./txt", exist_ok=True)

    # Open log file
    with open(log_filename, "w") as log_file:
        log_file.write("="*60 + "\n")
        log_file.write(f"CRAB Status Log - {datetime.datetime.now()}\n")
        log_file.write(f"Processing jobs matching: {keyword}\n")
        log_file.write("="*60 + "\n")

        # Get all CRAB jobs (filter if keyword is specified)
        crab_jobs = [os.path.join(crab_jobs_dir, d) for d in os.listdir(crab_jobs_dir)
            if os.path.isdir(os.path.join(crab_jobs_dir, d)) and keyword in d]
        if not crab_jobs:
            print(f"[!] No CRAB jobs found in '{crab_jobs_dir}' matching '{keyword}'.")
            return

        totalJOBS           = len(crab_jobs)
        counter             = 0
        total_RUNNING_jobs  = 0
        total_FINISHED_jobs = 0
        total_FAILED_jobs   = 0

        for job_dir in crab_jobs:
            print(f"\n'crab status -d {job_dir}'")
            log_file.write(f"\nChecking status of {job_dir}...\n")

            # Run crab status command
            status_cmd = f"crab status -d {job_dir}"
            result = subprocess.run(status_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if result.returncode != 0:
                print(f"[X] Error retrieving status. ")
                continue

            for line in result.stdout.split("\n"):
                if "Output dataset:" in line:
                    output_dir = line.split("Output dataset:")[-1].strip()
                    output_datasets.append(output_dir)
                    break

            # If job is finished, skip resubmission
            
            if "finished     		100.0%" in result.stdout:
                counter +=1
                if "done         		100.0%" or "publication has been disabled in the CRAB configuration file" in result.stdout:
                    print(f"TASK DONE!")
                    log_file.write(f"TASK DONE! \n")

                else:
                    print(f"[..] Saving outputs.")
                    log_file.write(f"[..] Saving outputs.")
                    continue

            log_file.write(result.stdout + "\n")
            log_file.write("#"*40 + "\n")
            log_file.write("#"*40 + "\n\n")

            for line in result.stdout.split("\n"):

                if "Publication status of" in line:
                    break

                if "finished     	" in line:  
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    FINISHED_jobs = parse_jobs_count(raw)
                    total_FINISHED_jobs += FINISHED_jobs
                    print(f"{CYAN} {FINISHED_jobs} jobs FINISHED.{RESET}")

                if "running      	" in line:
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    RUNNING_jobs = parse_jobs_count(raw)
                    print(f"{GREEN} {RUNNING_jobs} jobs RUNNING.    {RESET}")
                    total_RUNNING_jobs += RUNNING_jobs

                if "transferring 		" in line:
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    TRANSFERRING_jobs = parse_jobs_count(raw)
                    print(f"{GREEN} {TRANSFERRING_jobs} jobs TRANSFERRING.{RESET}")

                if "idle         	" in line:  
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    IDLE_jobs = parse_jobs_count(raw)
                    print(f"{GREY} {IDLE_jobs} jobs IDLE.          {RESET}")

                if "unsubmitted  	" in line:
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    UNSUBMITTED_jobs = parse_jobs_count(raw)
                    print(f"{GREY} {UNSUBMITTED_jobs} jobs UNSUBMITTED. {RESET}")

                if "   failed   " in line:
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    FAILED_jobs = parse_jobs_count(raw)
                    total_FAILED_jobs += FAILED_jobs
                    print(f"{RED} {FAILED_jobs} jobs FAILED.{RESET}")

                    if RE_SUBMIT:
                        print(f"{ORANGE}    [->] Resubmitting failed jobs.{RESET}")
                        log_file.write(f"[->] Resubmitting failed jobs. \n")
                        resubmit_cmd = f"crab resubmit -d {job_dir} --maxmemory 3000 --maxjobruntime 300"  #more memory if needed
                        resubmit_result = subprocess.run(resubmit_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

                        if resubmit_result.returncode != 0:
                            print(f"{RED}    [X] Error resubmitting failed jobs. {RESET}")
                            log_file.write(f"[X] Error resubmitting failed jobs.\n\n")
                        else:
                            log_file.write("\n")
                    else:
                        print(f"{ORANGE}[->] Automatic REsubmission is FALSE.{RESET}")

                if "	toRetry" in line:
                    raw = re.search(r'\((.*?)\)', line).group(1)
                    RETRY_jobs = parse_jobs_count(raw)
                    print(f"{ORANGE} {RETRY_jobs} jobs to RETRY.{RESET}")

        print("\nAll tasks checked. See the log file for details:", log_filename)

    # Sort dataset paths alphabetically
    if len(output_datasets) > 0:
        output_datasets.sort()

        # Save sorted dataset paths to the log file
        with open(log_OUTPUT_filename, "w") as log_file2:
            for dataset in output_datasets:
                log_file2.write(dataset + "\n")

        print(f"Output directories saved to: {log_OUTPUT_filename}")
    else:
        print(f"Output directories not available.")

    print()
    print(f"Total CRAB TASKS {CYAN}processed: {counter/totalJOBS*100:.2f}%{RESET}")
    print(f"Total CRAB JOBS {GREEN}  running: {total_RUNNING_jobs}{RESET}")
    print(f"Total CRAB JOBS  finished: {total_FINISHED_jobs}{RESET}")
    print(f"Total CRAB JOBS {RED}   failed: {total_FAILED_jobs}{RESET}")
    

# Example usage:
if __name__ == "__main__":
    import sys

    allowed_steps = {"GEN", "SIM", "DIGI", "HLT", "RECO", "MINIAOD", "NTUPLE"}

    if len(sys.argv) < 2:
        print("Usage: python CrabTask_manager.py <STEP>")
        print("Allowed STEP values: GEN, SIM, DIGI, HLT, RECO, MINIAOD, NTUPLE")
        sys.exit(1)

    keyword = sys.argv[1].upper()

    if keyword not in allowed_steps:
        print(f"[X] Invalid STEP: {keyword}")
        print("Allowed STEP values: GEN, SIM, DIGI, HLT, RECO, MINIAOD, NTUPLE")
        sys.exit(1)

    manage_crab_tasks(keyword)