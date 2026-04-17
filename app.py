import asyncio
import csv
import json
import os
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker
from playwright.async_api import async_playwright

# =======================================================================
# 0. SETUP: Helper to create a dummy input.csv for testing
# =======================================================================
def create_dummy_csv():
    if not os.path.exists("input.csv"):
        with open("input.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id", "action", "target_url"])
            # Using safe, public example sites for the Playwright test
            writer.writerow(["101", "check_title", "https://example.com"])
            writer.writerow(["102", "check_title", "https://httpbin.org"])

# =======================================================================
# 1. ACTIVITIES (The "Lambdas" and Automation Workers)
# =======================================================================

@activity.defn
async def write_file_activity(data: list) -> str:
    """Lambda 2: Writes the incoming data to a local file."""
    print(f"[Activity: Write File] Saving {len(data)} records to disk...")
    output_file = "processed_output.json"
    
    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)
        
    print(f"[Activity: Write File] ✅ Saved to {output_file}")
    return output_file

@activity.defn
async def read_file_activity(file_path: str) -> list:
    """Lambda 3: Reads the file. Instead of pushing to an external queue, 
    we return the data to Temporal so it can queue the Playwright tasks."""
    print(f"[Activity: Read File] Reading from {file_path}...")
    
    with open(file_path, "r") as f:
        data = json.load(f)
        
    print("[Activity: Read File] ✅ Data loaded successfully.")
    return data

@activity.defn
async def playwright_automation_activity(task_data: dict) -> str:
    """The Playwright Worker: Triggered by Temporal for each row of data."""
    url = task_data.get('target_url')
    user = task_data.get('user_id')
    
    print(f"\n🚀 [Playwright] Starting browser for User {user} at {url}...")
    
    async with async_playwright() as p:
        # headless=False so you can visually see it working locally
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            await page.goto(url)
            title = await page.title()
            print(f"   [Playwright] ✅ Page loaded. Title: '{title}'")
            await asyncio.sleep(1) # Brief pause just so you can see it
            
            return f"Success: {url} -> {title}"
        except Exception as e:
            print(f"   [Playwright] ❌ Error loading {url}: {e}")
            raise e # Temporal will catch this and retry the activity automatically!
        finally:
            await browser.close()
            print(f"🛑 [Playwright] Closed browser for User {user}.\n")

# =======================================================================
# 2. WORKFLOW (The Orchestrator)
# =======================================================================

@workflow.defn
class E2EAutomationWorkflow:
    @workflow.run
    async def run(self, initial_csv_data: list) -> list:
        print("\n[Workflow] 🚦 Orchestration Started.")
        
        # Step 1: Write the file
        file_path = await workflow.execute_activity(
            write_file_activity,
            initial_csv_data,
            start_to_close_timeout=timedelta(seconds=10)
        )

        # Step 2: Read the file back (Simulating Lambda 3 processing)
        records_to_process = await workflow.execute_activity(
            read_file_activity,
            file_path,
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        # Step 3: Trigger Playwright for every record in the file
        # Temporal acts as our highly-resilient queue here.
        automation_results = []
        for record in records_to_process:
            result = await workflow.execute_activity(
                playwright_automation_activity,
                record,
                # If a website fails to load, Temporal retries for up to 2 minutes
                start_to_close_timeout=timedelta(minutes=2) 
            )
            automation_results.append(result)
            
        print("[Workflow] 🏁 Orchestration Complete.")
        return automation_results

# =======================================================================
# 3. CLIENT / TRIGGER (The Application Entry Point)
# =======================================================================

async def main():
    # 1. Setup dummy data
    create_dummy_csv()

    # 2. "Lambda 1" Logic: Read the initial trigger file
    print("[Trigger] Reading input.csv to kick off pipeline...")
    parsed_csv_data = []
    with open("input.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed_csv_data.append(row)

    # 3. Connect to local Temporal Server
    client = await Client.connect("localhost:7233")

    # 4. Start the Temporal Worker to listen for jobs
    worker = Worker(
        client,
        task_queue="automation-queue",
        workflows=[E2EAutomationWorkflow],
        activities=[
            write_file_activity, 
            read_file_activity, 
            playwright_automation_activity
        ],
    )
    
    # Run the worker in the background
    worker_task = asyncio.create_task(worker.run())

    # 5. Start the Workflow
    print("[Trigger] Submitting job to Temporal...")
    final_results = await client.execute_workflow(
        E2EAutomationWorkflow.run,
        parsed_csv_data,
        id="full-e2e-automation-001",
        task_queue="automation-queue",
    )

    print("\n==================================================")
    print("🎯 PIPELINE FINISHED SUCCESSFULLY")
    print("==================================================")
    for res in final_results:
        print(f" - {res}")
    
    # Gracefully shut down
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    # Prevent Playwright + Asyncio conflicts on Windows
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    asyncio.run(main())
