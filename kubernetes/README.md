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
- Apply the deployments for X services: `kubectl apply -f kubernetes/service-template.yaml`