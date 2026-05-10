## mini-k8ts Kubernetes Manifests

Apply in this order:

```bash
kubectl apply -f deployments/k8s/namespace.yaml
kubectl apply -f deployments/k8s/configmap.yaml
kubectl apply -f deployments/k8s/secret.yaml
kubectl apply -f deployments/k8s/postgres.yaml
kubectl apply -f deployments/k8s/scheduler.yaml
kubectl apply -f deployments/k8s/workers.yaml
```

Notes:
- Workers expect access to the Docker socket via `hostPath`, which is only suitable for local/dev clusters.
- These manifests are a deployment baseline, not a production HA setup.
