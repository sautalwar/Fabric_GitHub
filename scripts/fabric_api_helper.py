"""
Fabric REST API Helper
Demonstrates key Fabric REST API operations for CI/CD automation.
Designed to be used in Fabric notebooks or standalone Python scripts.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class FabricApiClient:
    """Convenience wrapper for common Microsoft Fabric REST API operations."""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        base_url: str = "https://api.fabric.microsoft.com/v1",
        timeout: int = 60,
        session: Optional[Session] = None,
    ) -> None:
        """Initialize the client with service principal credentials and API settings."""
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._job_item_lookup: Dict[str, str] = {}

        retry_strategy = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST", "PUT", "PATCH", "DELETE"]),
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @property
    def credentials_configured(self) -> bool:
        """Return True when environment variables have been replaced with real credentials."""
        values = (self.tenant_id, self.client_id, self.client_secret)
        return all(values) and not any(str(value).startswith("<") for value in values)

    def get_access_token(self, force_refresh: bool = False) -> str:
        """Authenticate with Microsoft Entra ID and return a cached bearer token."""
        if not self.credentials_configured:
            raise ValueError(
                "Fabric credentials are not configured. Set FABRIC_TENANT_ID, FABRIC_CLIENT_ID, and "
                "FABRIC_CLIENT_SECRET before calling the API."
            )

        if not force_refresh and self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token

        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://api.fabric.microsoft.com/.default",
        }

        try:
            response = self.session.post(token_url, data=data, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            detail = exc.response.text if exc.response is not None else str(exc)
            raise RuntimeError(f"Failed to acquire Fabric access token: {detail}") from exc

        payload = response.json()
        self._access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._token_expires_at = time.time() + expires_in
        print(f"Authenticated successfully. Token valid for ~{expires_in} seconds.")
        return self._access_token

    def _make_request(
        self,
        method: str,
        endpoint: str,
        print_response: bool = True,
        return_response: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Send an authenticated request to the Fabric API with retries and rich errors."""
        token = self.get_access_token()
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", f"Bearer {token}")
        headers.setdefault("Accept", "application/json")
        if "json" in kwargs:
            headers.setdefault("Content-Type", "application/json")

        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                headers=headers,
                timeout=kwargs.pop("timeout", self.timeout),
                **kwargs,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Request to {url} failed before receiving a response: {exc}") from exc

        payload = self._extract_response_payload(response)
        if not response.ok:
            detail = payload if payload else {"message": response.text}
            detail_text = json.dumps(detail, indent=2) if isinstance(detail, (dict, list)) else str(detail)
            raise RuntimeError(
                f"Fabric API request failed: {method.upper()} {url} returned "
                f"{response.status_code} {response.reason}. Details: {detail_text}"
            )

        if print_response:
            if payload:
                self._print_json(f"{method.upper()} {url}", payload)
            else:
                summary = {
                    "statusCode": response.status_code,
                    "location": response.headers.get("Location"),
                    "operationId": response.headers.get("x-ms-operation-id"),
                    "retryAfter": response.headers.get("Retry-After"),
                }
                self._print_json(f"{method.upper()} {url}", summary)

        if return_response:
            return response, payload
        return payload

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """List the workspaces visible to the service principal."""
        payload = self._make_request("GET", "workspaces")
        workspaces = [
            {
                "id": item.get("id"),
                "displayName": item.get("displayName"),
                "type": item.get("type"),
            }
            for item in self._extract_value(payload)
        ]
        self._print_json("Workspace summary", workspaces)
        return workspaces

    def get_workspace_items(self, workspace_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Return notebook and lakehouse items for the specified workspace."""
        lakehouses_payload = self._make_request("GET", f"workspaces/{workspace_id}/lakehouses")
        notebooks_payload = self._make_request("GET", f"workspaces/{workspace_id}/notebooks")

        lakehouses = [self._simplify_item(item, fallback_type="Lakehouse") for item in self._extract_value(lakehouses_payload)]
        notebooks = [self._simplify_item(item, fallback_type="Notebook") for item in self._extract_value(notebooks_payload)]
        items = {"lakehouses": lakehouses, "notebooks": notebooks}
        self._print_json("Workspace items summary", items)
        return items

    def trigger_notebook_run(
        self,
        workspace_id: str,
        notebook_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Trigger a Fabric notebook run and return the job instance metadata."""
        body: Dict[str, Any] = {}
        if parameters:
            body["executionData"] = {"parameters": parameters}

        response, payload = self._make_request(
            "POST",
            f"workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook",
            json=body,
            return_response=True,
        )

        location = response.headers.get("Location") or response.headers.get("location")
        job_id = None
        if isinstance(payload, dict):
            job_id = payload.get("id") or payload.get("jobId") or payload.get("jobInstanceId")
        if not job_id and location:
            job_id = location.rstrip("/").split("/")[-1]

        if job_id:
            self._job_item_lookup[job_id] = notebook_id

        result = {
            "statusCode": response.status_code,
            "jobId": job_id,
            "location": location,
            "operationId": response.headers.get("x-ms-operation-id"),
            "response": payload,
        }
        self._print_json("Notebook run submission", result)
        return result

    def poll_job_status(
        self,
        workspace_id: str,
        job_id: str,
        timeout: int = 600,
        interval: int = 15,
    ) -> Dict[str, Any]:
        """Poll a notebook job until it reaches a terminal state or times out."""
        item_id = self._job_item_lookup.get(job_id)
        deadline = time.time() + timeout
        active_states = {"NotStarted", "Queued", "Running", "InProgress", "Started"}

        while time.time() < deadline:
            if item_id:
                endpoint = f"workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_id}"
            else:
                endpoint = f"operations/{job_id}"

            payload = self._make_request("GET", endpoint, print_response=False)
            status = payload.get("status", "Unknown") if isinstance(payload, dict) else "Unknown"
            print(f"Job {job_id} status: {status}")

            if status not in active_states:
                self._print_json("Final job status", payload)
                return payload

            time.sleep(interval)

        raise TimeoutError(f"Timed out waiting for job {job_id} after {timeout} seconds.")

    def git_connect(self, workspace_id: str, git_provider_details: Dict[str, Any]) -> Dict[str, Any]:
        """Connect a workspace to Git using provider details and optional credentials metadata."""
        if "gitProviderDetails" in git_provider_details:
            body = git_provider_details
        else:
            body = {"gitProviderDetails": git_provider_details}
        return self._make_request("POST", f"workspaces/{workspace_id}/git/connect", json=body)

    def git_commit(
        self,
        workspace_id: str,
        commit_message: str,
        items: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """Commit all changes or a selected set of workspace items to the connected branch."""
        body: Dict[str, Any] = {
            "mode": "Selective" if items else "All",
            "comment": commit_message,
        }
        if items:
            body["items"] = items
        return self._make_request("POST", f"workspaces/{workspace_id}/git/commitToGit", json=body)

    def git_update_from_git(self, workspace_id: str) -> Dict[str, Any]:
        """Initialize a Git connection and, if needed, pull remote changes into the workspace."""
        initialize_response = self._make_request(
            "POST",
            f"workspaces/{workspace_id}/git/initializeConnection",
            json={},
        )

        required_action = initialize_response.get("requiredAction") or initialize_response.get("RequiredAction")
        if required_action not in {"UpdateFromGit", "CommitToGit"}:
            summary = {
                "requiredAction": required_action or "None",
                "message": "No Git update was required.",
            }
            self._print_json("Update from Git summary", summary)
            return summary

        if required_action == "CommitToGit":
            summary = {
                "requiredAction": required_action,
                "message": "Workspace contains uncommitted changes. Commit them before updating from Git.",
                "initializeConnection": initialize_response,
            }
            self._print_json("Update from Git summary", summary)
            return summary

        body = {
            "remoteCommitHash": initialize_response.get("remoteCommitHash") or initialize_response.get("RemoteCommitHash"),
            "workspaceHead": initialize_response.get("workspaceHead") or initialize_response.get("WorkspaceHead"),
        }
        response, payload = self._make_request(
            "POST",
            f"workspaces/{workspace_id}/git/updateFromGit",
            json=body,
            return_response=True,
        )
        result = {
            "statusCode": response.status_code,
            "operationId": response.headers.get("x-ms-operation-id"),
            "retryAfter": response.headers.get("Retry-After"),
            "response": payload,
        }
        self._print_json("Update from Git submission", result)
        return result

    def deploy_pipeline(
        self,
        pipeline_id: str,
        source_stage: str,
        target_stage: str,
        items: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Deploy all or selected items from one deployment pipeline stage to another."""
        body: Dict[str, Any] = {
            "sourceStageId": source_stage,
            "targetStageId": target_stage,
        }
        if items:
            body["items"] = items

        response, payload = self._make_request(
            "POST",
            f"deploymentPipelines/{pipeline_id}/deploy",
            json=body,
            return_response=True,
        )
        result = {
            "statusCode": response.status_code,
            "operationId": response.headers.get("x-ms-operation-id"),
            "retryAfter": response.headers.get("Retry-After"),
            "response": payload,
        }
        self._print_json("Deployment submission", result)
        return result

    def list_deployment_pipelines(self) -> List[Dict[str, Any]]:
        """List deployment pipelines available to the caller."""
        payload = self._make_request("GET", "deploymentPipelines")
        pipelines = [
            {
                "id": item.get("id"),
                "displayName": item.get("displayName"),
                "description": item.get("description"),
            }
            for item in self._extract_value(payload)
        ]
        self._print_json("Deployment pipeline summary", pipelines)
        return pipelines

    @staticmethod
    def _extract_value(payload: Any) -> List[Dict[str, Any]]:
        """Normalize Fabric list responses to a Python list."""
        if isinstance(payload, dict) and isinstance(payload.get("value"), list):
            return payload["value"]
        if isinstance(payload, list):
            return payload
        return []

    @staticmethod
    def _extract_response_payload(response: Response) -> Any:
        """Return JSON content when possible, otherwise return plain text or an empty dict."""
        if not response.content:
            return {}
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type.lower():
            return response.json()
        text = response.text.strip()
        return {"text": text} if text else {}

    @staticmethod
    def _simplify_item(item: Dict[str, Any], fallback_type: str) -> Dict[str, Any]:
        """Return a compact item representation suitable for demo output."""
        return {
            "id": item.get("id"),
            "displayName": item.get("displayName"),
            "type": item.get("type") or fallback_type,
        }

    @staticmethod
    def _print_json(title: str, payload: Any) -> None:
        """Pretty-print JSON for demos and notebook visibility."""
        print(f"\n--- {title} ---")
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    client = FabricApiClient(
        tenant_id=os.getenv("FABRIC_TENANT_ID", "<your-tenant-id>"),
        client_id=os.getenv("FABRIC_CLIENT_ID", "<your-client-id>"),
        client_secret=os.getenv("FABRIC_CLIENT_SECRET", "<your-client-secret>"),
    )

    if not client.credentials_configured:
        print("Set FABRIC_TENANT_ID, FABRIC_CLIENT_ID, and FABRIC_CLIENT_SECRET to run live Fabric API demos.")
    else:
        print("=== Listing Workspaces ===")
        workspaces = client.list_workspaces()
        for workspace in workspaces:
            print(f"  {workspace['displayName']} ({workspace['id']})")

        print("\n=== Listing Deployment Pipelines ===")
        pipelines = client.list_deployment_pipelines()
        for pipeline in pipelines:
            print(f"  {pipeline['displayName']} ({pipeline['id']})")

        if workspaces:
            print("\n=== Listing Items In First Workspace ===")
            first_workspace_id = workspaces[0]["id"]
            client.get_workspace_items(first_workspace_id)

        print("\n=== Notebook Run Example ===")
        print("Set FABRIC_NOTEBOOK_ID and uncomment the lines below to trigger a notebook run.")
        # notebook_id = os.getenv("FABRIC_NOTEBOOK_ID")
        # if notebook_id:
        #     job = client.trigger_notebook_run(first_workspace_id, notebook_id, parameters={"environment": "DEV"})
        #     if job.get("jobId"):
        #         client.poll_job_status(first_workspace_id, job["jobId"])

        print("\n=== Git Automation Examples ===")
        print("client.git_connect(workspace_id='...', git_provider_details={...})")
        print("client.git_commit(workspace_id='...', commit_message='Demo commit from Fabric API helper')")
        print("client.git_update_from_git(workspace_id='...')")

        print("\n=== Deployment Pipeline Example ===")
        print("client.deploy_pipeline(pipeline_id='...', source_stage='...', target_stage='...')")
