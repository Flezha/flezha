# Kubernetes resource files

These files can be used as a reference for deploying the application to a Kubernetes cluster.
It creates a namespace, role, service account, role binding and a statefulset.

## Setup
- Apply the namespace: `kubectl apply -f flezha-namespace.yaml`
- Apply the role: `kubectl apply -f flezha-role.yaml`
- Apply the service account: `kubectl apply -f flezha-serviceaccount.yaml`
- Apply the role binding: `kubectl apply -f flezha-rolebinding.yaml`

## Deploy
- Apply the daemonset for the flezha agents: `kubectl apply -f kubernetes/flezha-daemonset.yaml`
- Apply the deployments for X(compute_engines) services: `kubectl apply -f kubernetes/service-template.yaml`

## Logs

Agent logs are persisted on the host machine at `/opt/flezha-logs/`.

### Accessing Logs

**Real-time container logs (stdout):**
```bash
# List pods
kubectl get pods -n flezha -l app=flezha

# Follow logs from specific pod
kubectl logs -f -n flezha flezha-daemonset-XXXXX

# Follow logs from all agent pods
kubectl logs -f -n flezha -l app=flezha --all-containers=true
```

**Persistent log files:**
```bash
# Inside pod
kubectl exec -it -n flezha flezha-daemonset-XXXXX -- tail -f /var/log/flezha/flezha.log

# On host machine (direct access)
sudo tail -f /opt/flezha-logs/flezha.log

# Copy logs to local machine
kubectl cp -n flezha flezha-daemonset-XXXXX:/var/log/flezha/flezha.log ./agent-logs.log
```

### Log Configuration

Logs are configured via environment variables:
- `FLEZHA_LOG_PATH`: `/var/log/flezha/flezha.log` (file path)
- `FLEZHA_LOG_LEVEL`: `info` (default, can be: debug, info, warning, error)

The agent logs to both stdout (for `kubectl logs`) and file (for persistence).