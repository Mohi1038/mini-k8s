# mini-k8ts Dashboard

A lightweight read-only dashboard for the mini-k8ts scheduler API.

## Run locally

From the repo root:

```bash
python3 -m http.server 4173 -d frontend/dashboard
```

Then open:

```text
http://localhost:4173
```

By default the dashboard connects to:

```text
http://localhost:8080
```

## Current scope

- cluster overview
- nodes
- jobs
- tasks
- services
- namespaces
- insights

This dashboard is intentionally read-only for now so it can stay simple while the backend keeps maturing.
