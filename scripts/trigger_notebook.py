"""
Trigger Fabric Notebook via REST API
======================================
Authenticates using Azure Identity (OIDC or Service Principal) and
triggers a Fabric notebook execution via the REST API.

Usage:
  python scripts/trigger_notebook.py --env uat --workspace-id <ID> --notebook-name migration_runner

Used by: .github/workflows/deploy-fabric.yml (deploy-uat, deploy-prod jobs)
"""

import argparse
import json
import os
import sys
import time

try:
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("ERROR: azure-identity package not installed. Run: pip install azure-identity")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests package not installed. Run: pip install requests")
    sys.exit(1)


FABRIC_API_BASE = os.getenv("FABRIC_API_BASE", "https://api.fabric.microsoft.com/v1")
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Polling configuration
MAX_WAIT_SECONDS = 600  # 10 minutes
POLL_INTERVAL = 10      # seconds


def get_access_token() -> str:
    """Get access token using Azure Identity (supports OIDC, SP, CLI)."""
    credential = DefaultAzureCredential()
    token = credential.get_token(FABRIC_SCOPE)
    return token.token


def find_notebook_id(token: str, workspace_id: str, notebook_name: str) -> str:
    """Look up the notebook item ID by name."""
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    items = response.json().get("value", [])
    for item in items:
        if item["displayName"] == notebook_name and item["type"] == "Notebook":
            return item["id"]
    
    raise ValueError(f"Notebook '{notebook_name}' not found in workspace {workspace_id}")


def trigger_notebook(token: str, workspace_id: str, notebook_id: str) -> str:
    """Trigger notebook execution and return the job instance URL."""
    url = (
        f"{FABRIC_API_BASE}/workspaces/{workspace_id}"
        f"/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    print(f"  POST {url}")
    response = requests.post(url, headers=headers, timeout=30)
    
    if response.status_code == 202:
        location = response.headers.get("Location", "")
        print(f"  ✅ Job accepted (202)")
        print(f"  📍 Location: {location}")
        return location
    else:
        print(f"  ❌ Unexpected status: {response.status_code}")
        print(f"  Response: {response.text}")
        response.raise_for_status()


def poll_job_status(token: str, location_url: str) -> bool:
    """Poll the job until completion."""
    headers = {"Authorization": f"Bearer {token}"}
    elapsed = 0
    
    while elapsed < MAX_WAIT_SECONDS:
        response = requests.get(location_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            status = data.get("status", "Unknown")
            print(f"  ⏳ Status: {status} ({elapsed}s elapsed)")
            
            if status == "Completed":
                print("  ✅ Notebook execution completed successfully!")
                return True
            elif status in ("Failed", "Cancelled"):
                print(f"  ❌ Notebook execution {status}")
                error = data.get("failureReason", {})
                if error:
                    print(f"  Error: {json.dumps(error, indent=2)}")
                return False
        
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    
    print(f"  ⏰ Timeout after {MAX_WAIT_SECONDS}s — check Fabric portal for status")
    return False


def main():
    parser = argparse.ArgumentParser(description="Trigger a Fabric notebook via REST API")
    parser.add_argument("--env", required=True, choices=["dev", "uat", "prod"],
                        help="Target environment")
    parser.add_argument("--workspace-id", required=True,
                        help="Fabric workspace ID (GUID)")
    parser.add_argument("--notebook-name", default="migration_runner",
                        help="Name of the notebook to trigger")
    args = parser.parse_args()

    print("=" * 60)
    print(f"🚀 Triggering Fabric Notebook")
    print(f"   Environment: {args.env}")
    print(f"   Workspace:   {args.workspace_id}")
    print(f"   Notebook:    {args.notebook_name}")
    print("=" * 60)

    # Step 1: Authenticate
    print("\n🔐 Authenticating via Azure Identity (OIDC/SP)...")
    token = get_access_token()
    print("  ✅ Token acquired")

    # Step 2: Find notebook
    print(f"\n🔍 Looking up notebook '{args.notebook_name}'...")
    notebook_id = find_notebook_id(token, args.workspace_id, args.notebook_name)
    print(f"  ✅ Found: {notebook_id}")

    # Step 3: Trigger execution
    print(f"\n🏗️ Triggering notebook execution...")
    location = trigger_notebook(token, args.workspace_id, notebook_id)

    # Step 4: Poll for completion
    if location:
        print(f"\n⏳ Waiting for notebook to complete...")
        success = poll_job_status(token, location)
        if not success:
            sys.exit(1)
    
    print(f"\n✅ Deployment to {args.env} complete!")


if __name__ == "__main__":
    main()
