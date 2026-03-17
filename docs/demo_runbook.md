# Demo Runbook — Fabric CI/CD for Lakehouse Schema Evolution
> **Duration:** 60 minutes | **Audience:** Digital Realty engineering team  
> **Presenter:** [Your Name] | **Date:** [Date]

## Quick URLs
- [ ] **Navigate** to Fabric portal — https://app.fabric.microsoft.com/home?experience=data-engineering
- [ ] **Navigate** to Deployment pipelines — https://app.fabric.microsoft.com/groups/me/deploymentpipelines?experience=data-engineering
- [ ] **Navigate** to Azure DevOps repo — https://dev.azure.com/<org>/<project>/_git/<repo>
- [ ] **Navigate** to Azure DevOps pipeline — https://dev.azure.com/<org>/<project>/_build?definitionId=<pipeline-id>
- [ ] **Navigate** to GitHub repo — https://github.com/<owner>/<repo>
- [ ] **Navigate** to GitHub Actions — https://github.com/<owner>/<repo>/actions
- [ ] **Navigate** to Fabric REST API docs — https://learn.microsoft.com/en-us/rest/api/fabric/
- [ ] **Navigate** to Fabric Git automation docs — https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation
- [ ] **Navigate** to Fabric deployment pipeline automation docs — https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/pipeline-automation-fabric

## Pre-Demo Checklist
- [ ] 3 Fabric workspaces created (`DigitalRealty-Dev`, `DigitalRealty-UAT`, `DigitalRealty-Prod`)
- [ ] Deployment Pipeline configured and mapped Dev → UAT → Prod
- [ ] Azure DevOps repo with demo files pushed
- [ ] GitHub repo with demo files pushed
- [ ] Dev workspace connected to Git
- [ ] `v001_baseline_tables` already applied in all 3 environments
- [ ] `v002_capacity_and_sustainability` ready to show in Git
- [ ] Service Principal registered and configured; client secret hidden
- [ ] Browser tabs pre-opened: Fabric portal, Azure DevOps, GitHub, docs
- [ ] PowerPoint open on Slide 1
- [ ] Presenter stopwatch ready

---

## Part 1: Slides (10 min)
### 0:00-2:00 — Slide 1: SDLC Overview
- [ ] **Show** Slide 1 and frame the Dev → UAT → Prod flow.
  > "This is the SDLC pattern our teams already use; Fabric needs to fit into it cleanly."
### 2:00-4:00 — Slide 2: What Git Tracks
- [ ] **Show** that notebooks, pipelines, semantic models, and reports are already Git-tracked.
  > "Fabric Git integration covers most artifacts well, so the repo becomes the source of truth."
### 4:00-7:00 — Slide 3: The Schema Gap
- [ ] **Say** that Lakehouse schema changes are the missing piece in normal Fabric promotion.
  > "The gap is Lakehouse schema evolution—new tables and altered schemas do not ride along automatically."
### 7:00-10:00 — Slides 4-5: Solution and Roadmap
- [ ] **Show** Slide 4 with schema-as-code: versioned SQL/PySpark migrations plus a runner notebook.
  > "We treat schema like application code: version it, review it, promote it, and re-run it safely."
- [ ] **Show** Slide 5 with the demo roadmap: Dev change, CI/CD, GitHub parity, REST APIs.
  > "Now I’ll walk the full path end to end."

## Part 2: Environment Tour (8 min)
### 10:00-11:30 — Fabric Workspaces
- [ ] **Navigate** to https://app.fabric.microsoft.com/home?experience=data-engineering and open Dev, UAT, then Prod.
  > "These 3 workspaces mirror our environments exactly: Dev for build, UAT for validation, Prod for release."
### 11:30-12:30 — Deployment Pipeline
- [ ] **Click** gear icon → Deployment pipelines → `DigitalRealty-Pipeline`.
  > "Fabric gives us native artifact promotion between workspaces."
### 12:30-13:30 — Azure DevOps Repo
- [ ] **Navigate** to https://dev.azure.com/<org>/<project>/_git/<repo> and **Show** `lakehouse-migrations`, `pipelines`, `scripts`, and `presentation`.
  > "The migration scripts and automation live in Git beside the rest of the solution."
### 13:30-14:30 — GitHub Repo
- [ ] **Navigate** to https://github.com/<owner>/<repo> and **Show** the same folder structure.
  > "The pattern is portable—we can run the same approach in Azure DevOps or GitHub."
### 14:30-15:30 — Git Integration
- [ ] **Click** Dev workspace → Settings → Git integration and confirm the repo connection.
  > "Dev is connected to Git, so authored Fabric artifacts stay synced with the repository."
### 15:30-16:30 — Service Principal
- [ ] **Show** the app registration or variable group name only; never reveal the secret.
  > "Automation authenticates non-interactively with a service principal, just like any enterprise release pipeline."
### 16:30-18:00 — Key Files
- [ ] **Show** `v001_baseline_tables.sql`, `v002_capacity_and_sustainability.sql`, `migration_runner.py`, and `migration_state\applied_migrations.json`.
  > "These files give us versioning, execution logic, and a durable record of what ran where."

## Part 3: Happy Path in Dev (8 min)
### 18:00-19:00 — Baseline State
- [ ] **Navigate** to Dev workspace → Lakehouse → Tables and **Show** the baseline tables already present.
  > "We start with a working baseline, so the audience can clearly see the delta we introduce."
### 19:00-20:30 — New Migration in Git
- [ ] **Show** `lakehouse-migrations\migrations\v002_capacity_and_sustainability.sql` and/or `.py`.
  > "This new migration adds capacity and sustainability tables as a normal code change."
### 20:30-21:30 — Versioning Convention
- [ ] **Say** why the `v001`, `v002` naming pattern makes execution order explicit and repeatable.
  > "Versioned filenames make the migration order obvious and safe to automate."
### 21:30-23:00 — Sync Dev Artifacts
- [ ] **Click** Sync in the Dev workspace if you want to show the Git pull into Fabric.
  > "Artifact sync is native; schema application is the extra step we are adding."
### 23:00-24:30 — Run Migration Runner
- [ ] **Click** `migration_runner.py` in Fabric and **Click** Run to apply pending migrations in Dev.
  > "The notebook reads pending migrations and applies only what has not yet run."
### 24:30-26:00 — Verify Dev Result
- [ ] **Click** Refresh on Tables and **Show** `capacity_utilization` and `sla_incidents` in Dev.
  > "That is the key moment: the schema change is now visible in the Lakehouse and traceable back to Git."

## Part 4: Azure DevOps CI/CD to UAT and Prod (12 min)
### 26:00-27:00 — Open Pipeline
- [ ] **Navigate** to https://dev.azure.com/<org>/<project>/_build?definitionId=<pipeline-id> and open the latest `azure-pipelines.yml` run.
  > "Now we move from a Dev-only change to governed promotion."
### 27:00-28:30 — Stage Layout
- [ ] **Show** the UAT stage, approval gate, and Prod stage in the pipeline graph.
  > "The pipeline mirrors the same controls we use for application releases."
### 28:30-30:00 — UAT Execution
- [ ] **Show** the UAT job logs and point out authentication, REST API calls, and notebook execution.
  > "The pipeline does not click through the UI; it uses APIs so the process is repeatable."
### 30:00-31:30 — UAT Verification
- [ ] **Navigate** to UAT workspace → Lakehouse → Tables and **Show** the new tables.
  > "UAT now matches Dev for both artifacts and schema."
### 31:30-33:00 — Approval Gate
- [ ] **Say** who would normally approve promotion to Prod and why the gate matters.
  > "This is where release governance stays intact—nothing goes to Prod without human sign-off."
### 33:00-34:30 — Prod Execution
- [ ] **Click** Approve or open the completed Prod stage and **Show** the same automation pattern.
  > "Prod uses the exact same code path, which removes environment drift."
### 34:30-36:00 — Prod Verification
- [ ] **Navigate** to Prod workspace → Lakehouse → Tables and **Show** `capacity_utilization` and `sla_incidents`.
  > "All 3 environments are now aligned through code, automation, and approvals."

## Part 5: GitHub Actions Variant (11 min)
### 36:00-37:30 — GitHub Repo View
- [ ] **Navigate** to https://github.com/<owner>/<repo> and **Show** `.github/workflows`, `lakehouse-migrations`, and `scripts`.
  > "We are not locked into one CI/CD platform; the same repo structure works in GitHub too."
### 37:30-39:00 — Workflow File
- [ ] **Show** the workflow file and point out UAT and Prod jobs.
  > "The logic is the same: authenticate, call Fabric, run migrations, validate, then promote."
### 39:00-40:30 — Actions Run
- [ ] **Navigate** to https://github.com/<owner>/<repo>/actions and open a successful workflow run.
  > "Here is the GitHub-native execution view for teams standardizing on Actions."
### 40:30-42:00 — Platform Comparison
- [ ] **Show** that Azure DevOps and GitHub both call the same migration scripts and runner notebook.
  > "The CI engine changes, but the schema-as-code pattern stays the same."
### 42:00-47:00 — Branch and PR Story
- [ ] **Say** the normal change flow: branch → PR review → merge → pipeline run → Fabric promotion.
  > "This fits into existing engineering controls instead of asking teams to invent a Fabric-only process."

## Part 6: REST APIs and Automation Hooks (8 min)
### 47:00-48:30 — Why APIs
- [ ] **Say** why REST APIs matter before opening any code or docs.
  > "APIs are the glue that makes this demo enterprise-ready: no manual clicks required for release automation."
### 48:30-50:00 — Helper Scripts
- [ ] **Show** the `scripts` folder and the helper script or request collection you will reference.
  > "These helpers wrap authentication, notebook execution, and deployment actions."
### 50:00-51:30 — Notebook Execution API
- [ ] **Navigate** to https://learn.microsoft.com/en-us/rest/api/fabric/ and **Show** the notebook execution endpoint.
  > "This is how the pipeline remotely starts the migration runner in the target workspace."
### 51:30-53:00 — Git / Sync API
- [ ] **Navigate** to https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation and **Show** the Git sync automation docs.
  > "Git sync can be automated too, so even the repo-to-workspace step can be scripted."
### 53:00-54:30 — Deployment Pipeline API
- [ ] **Navigate** to https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/pipeline-automation-fabric and **Show** the promotion docs.
  > "Promotion becomes an auditable API-driven step, not an operator memory exercise."
### 54:30-55:00 — Close the Loop
- [ ] **Say** the 3 control points: versioned migration, automated execution, verified promotion.
  > "Once those 3 pieces are in place, Lakehouse schema evolution fits naturally into CI/CD."

## Wrap-Up (5 min)
### 55:00-57:00 — Summary
- [ ] **Say**: Schema-as-Code + CI/CD + REST APIs = full coverage for Fabric Lakehouse promotion.
  > "We close the schema gap without abandoning Git, approvals, or automation standards."
### 57:00-58:30 — Final Visual
- [ ] **Show** the architecture or solution slide one more time.
  > "One picture to remember: Git is the source of truth, pipelines orchestrate, Fabric executes."
### 58:30-60:00 — Q&A
- [ ] **Say** "Happy to go deeper on governance, APIs, or the migration runner implementation."
  > "Keep Fabric, Azure DevOps, and GitHub tabs ready for fast call-backs."

## Presenter Notes
- [ ] **Show** browser zoom at 125%+ and collapse side panes before tab switches.
- [ ] **Say** less while pages load; let the visual do the work.
- [ ] **Click** into completed runs if live execution is slow.
- [ ] **Say** no secrets, tenant IDs, client secrets, or approval emails on screen.
- [ ] **Say** if time runs short, compress Part 5 and preserve Part 6 plus Wrap-Up.
