---
name: bounded-memory-under-pressure
description: >-
  Run the bounded-memory-under-pressure tests for a user: memory and CPU of LaserData's server (Laser Streams) and of Laser SDK clients while a consumer catches up on a large backlog, while a producer's server stops answering for 60 seconds, and across a graceful and a crash restart. Runs on Google Cloud VMs with one command, or against the user's LaserData Cloud deployment. Use when the user asks to run, repeat or change these tests, or to measure LaserData memory or CPU under load.
---

# bounded-memory-under-pressure

You are running this folder's tests for the user. Read `README.md` in this folder first. Report what each step did in one line, and wait for approval before anything that creates resources or costs money.

## 1. Pick where to run

Ask the user, unless they already said:

- **Google Cloud** (the measured route; the published results come from it). Needs `gcloud` signed in to a project where they can create VMs.
- **LaserData Cloud** (their own managed deployment). **Needs a paid tier** (Standard or higher, with clustering on the organization's plan). The Free starter is capped at 200 KB/s and one node, which is too small for these tests.

## 2a. Google Cloud

1. Check `gcloud config get-value project` and `gcloud auth list`. If not signed in, ask the user to run `gcloud auth login` in their own terminal.
2. Say what it creates before running: one server VM and one client VM per test (n2-standard-4, 100 GB pd-ssd on the server), all running at once, deleted at the end even if the run fails.
3. Offer a quick trial first:
   ```sh
   PROJECT=<project> FOOTPRINT_SCALE=0.1 gcp-vm/run-gcp.sh
   ```
   Then the full run:
   ```sh
   PROJECT=<project> gcp-vm/run-gcp.sh
   ```
   One test only: `TESTS="restart"` (choices: `backlog-2 backlog-10 backlog-50 producer restart`). Another zone: `ZONE=us-central1-a`.
4. Results land in `gcp-vm/results/<profile>/`: a `README.md` with the tables, `summary.json`, and charts in `charts/`. Show the user the tables and the charts.
5. Afterwards confirm nothing is left running:
   ```sh
   gcloud compute instances list --filter="labels.purpose=laserdata-footprint"
   ```

## 2b. LaserData Cloud (needs a paid tier)

1. Get a deployment and its connection details with `/laser-quickstart` from the LaserData CLI skills (`laserdata/laser-cli-claude`). Until that skill is available, use `/laser-onboard`, `/laser-deploy`, `/laser-access` and `/laser-credentials` in that order. Price the deployment first and get a yes.
2. The user stores the connection string in an env file with mode 600, for example `~/.config/laserdata/footprint.env` containing `LASER_CONNECTION_STRING=...`. Never ask for it in the chat.
3. The Laser SDK clients in `footprint/` connect with `LASER_CONNECTION_STRING`, so client memory and CPU are measured the same way.
4. Server memory and CPU come from the deployment's monitoring (`/laser-troubleshoot`, `laser deployment monitor`), not from Docker.
5. The producer outage and restart tests stop and restart the server through Docker on the server host. A managed deployment does not expose that, so tell the user these two run on the Google Cloud route.
6. When done, offer to delete the deployment through `/laser-deploy`. Paid tiers bill by the second.

## Rules

- Change one thing at a time (partitions, record size, rate, memory limit) and say what changed.
- Never edit the numbers in `results/`. The scripts write them from the raw samples.
- A run where a test reports missing records is a failed run. Report it; do not publish it.
- Never put an API key, password or connection string with a password in the chat or on a command line.

## Look things up

Use the LaserData docs MCP server to check Laser SDK calls instead of guessing:

```sh
claude mcp add --transport http laserdata-docs https://laserdata-docs-mcp.fly.dev/mcp
```
