"""Example script that demonstrates workspace setup tasks with the Fabric REST API."""

from __future__ import annotations

import os
from typing import Any, Dict, List

from fabric_api_helper import FabricApiClient


def _find_workspace(workspaces: List[Dict[str, Any]], display_name: str) -> Dict[str, Any] | None:
    """Return a workspace by display name if it exists."""
    for workspace in workspaces:
        if workspace.get("displayName") == display_name:
            return workspace
    return None


def setup_demo_environment() -> None:
    """Demonstrate how to provision and configure Fabric workspaces for the demo."""
    client = FabricApiClient(
        tenant_id=os.getenv("FABRIC_TENANT_ID", "<your-tenant-id>"),
        client_id=os.getenv("FABRIC_CLIENT_ID", "<your-client-id>"),
        client_secret=os.getenv("FABRIC_CLIENT_SECRET", "<your-client-secret>"),
    )

    print("=== Fabric Demo Environment Setup ===")
    if not client.credentials_configured:
        print("Set FABRIC_TENANT_ID, FABRIC_CLIENT_ID, and FABRIC_CLIENT_SECRET to run this setup demo.")
        return

    print("\n1. Listing existing workspaces")
    workspaces = client.list_workspaces()
    for workspace in workspaces:
        print(f"  - {workspace['displayName']} [{workspace['id']}]")

    print("\n2. Workspace creation examples (commented out by default)")
    workspace_payloads = [
        {"displayName": "Fabric Demo - Dev", "description": "Development workspace for Fabric CI/CD demos."},
        {"displayName": "Fabric Demo - UAT", "description": "UAT workspace for Fabric CI/CD demos."},
        {"displayName": "Fabric Demo - Prod", "description": "Production workspace for Fabric CI/CD demos."},
    ]
    client._print_json("Suggested workspace payloads", workspace_payloads)
    print("# Requires workspace creation permissions. Uncomment when ready to provision workspaces:")
    print("# for payload in workspace_payloads:")
    print("#     client._make_request('POST', 'workspaces', json=payload)")

    print("\n3. Git connection example")
    dev_workspace_name = os.getenv("FABRIC_DEV_WORKSPACE_NAME", "Fabric Demo - Dev")
    dev_workspace = _find_workspace(workspaces, dev_workspace_name) or (workspaces[0] if workspaces else None)
    git_connect_body = {
        "gitProviderDetails": {
            "gitProviderType": os.getenv("FABRIC_GIT_PROVIDER", "GitHub"),
            "organizationName": os.getenv("FABRIC_GIT_ORGANIZATION", "<org-name>"),
            "projectName": os.getenv("FABRIC_GIT_PROJECT", "<project-name-if-ado>"),
            "repositoryName": os.getenv("FABRIC_GIT_REPOSITORY", "Fabric_GitHub"),
            "branchName": os.getenv("FABRIC_GIT_BRANCH", "main"),
            "directoryName": os.getenv("FABRIC_GIT_DIRECTORY", "/"),
        },
        "myGitCredentials": {
            "source": "ConfiguredConnection",
            "connectionId": os.getenv("FABRIC_GIT_CONNECTION_ID", "<connection-id>"),
        },
    }
    client._print_json("Git connect request example", git_connect_body)
    if dev_workspace:
        print(f"Target workspace for Git connection: {dev_workspace['displayName']} ({dev_workspace['id']})")
        print("# Uncomment after populating the Git details and connection ID:")
        print(f"# client.git_connect('{dev_workspace['id']}', git_connect_body)")
    else:
        print("No workspace found yet. Create the Dev workspace first, then connect it to Git.")

    print("\n4. Deployment pipeline stage configuration examples")
    pipelines = client.list_deployment_pipelines()
    if pipelines:
        pipeline = pipelines[0]
        print(f"Using pipeline example: {pipeline['displayName']} ({pipeline['id']})")
        print("Populate the stage IDs and workspace IDs below to map environments to the pipeline.")
        print("# Example stage assignments:")
        print(
            f"# client._make_request('POST', 'deploymentPipelines/{pipeline['id']}/stages/<dev-stage-id>/assignWorkspace', json={{'workspaceId': '<dev-workspace-id>'}})"
        )
        print(
            f"# client._make_request('POST', 'deploymentPipelines/{pipeline['id']}/stages/<uat-stage-id>/assignWorkspace', json={{'workspaceId': '<uat-workspace-id>'}})"
        )
        print(
            f"# client._make_request('POST', 'deploymentPipelines/{pipeline['id']}/stages/<prod-stage-id>/assignWorkspace', json={{'workspaceId': '<prod-workspace-id>'}})"
        )
    else:
        print("No deployment pipelines found. Create one in the Fabric UI or via the REST API before assigning stages.")

    print("\nSetup demo complete. Review the printed payloads, then uncomment the calls you want to execute.")


if __name__ == "__main__":
    setup_demo_environment()
