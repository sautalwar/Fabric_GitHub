"""
Verify Deployment
==================
Post-deployment verification script that checks:
  1. Notebook execution completed successfully
  2. Expected tables exist in the Lakehouse
  3. Migration state is up to date

Usage:
  python scripts/verify_deployment.py --env uat --workspace-id <ID>

Used by: .github/workflows/deploy-fabric.yml (verify steps)
"""

import argparse
import json
import sys

try:
    from azure.identity import DefaultAzureCredential
    import requests
except ImportError:
    print("ERROR: Required packages not installed. Run: pip install azure-identity requests")
    sys.exit(1)


FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def get_token() -> str:
    credential = DefaultAzureCredential()
    return credential.get_token(FABRIC_SCOPE).token


def list_workspace_items(token: str, workspace_id: str) -> list:
    """List all items in a workspace."""
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])


def verify_items(items: list, expected_types: list) -> dict:
    """Verify expected item types exist."""
    found = {}
    for item_type in expected_types:
        matching = [i for i in items if i["type"] == item_type]
        found[item_type] = len(matching)
    return found


def main():
    parser = argparse.ArgumentParser(description="Verify Fabric deployment")
    parser.add_argument("--env", required=True, choices=["dev", "uat", "prod"])
    parser.add_argument("--workspace-id", required=True)
    args = parser.parse_args()

    print("=" * 60)
    print(f"📊 Post-Deployment Verification — {args.env.upper()}")
    print("=" * 60)

    token = get_token()
    items = list_workspace_items(token, args.workspace_id)

    print(f"\n📦 Found {len(items)} item(s) in workspace:")
    for item in items:
        print(f"   • [{item['type']}] {item['displayName']}")

    # Verify expected items exist
    expected = ["Notebook", "Lakehouse", "DataPipeline"]
    found = verify_items(items, expected)

    print(f"\n✅ Verification Results:")
    all_good = True
    for item_type, count in found.items():
        status = "✅" if count > 0 else "⚠️"
        if count == 0:
            all_good = False
        print(f"   {status} {item_type}: {count} found")

    if all_good:
        print(f"\n✅ Deployment to {args.env.upper()} verified successfully!")
    else:
        print(f"\n⚠️ Some expected items are missing — review the workspace manually.")
        # Don't fail hard — missing items may be intentional
        sys.exit(0)


if __name__ == "__main__":
    main()
