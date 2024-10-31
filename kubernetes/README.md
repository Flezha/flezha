# Kubernetes resource files

These files can be used as a reference for deploying the application to a Kubernetes cluster.
It creates a namespace, role, service account, role binding and a statefulset.

## Setup
- Apply the namespace: `kubectl apply -f aflecha-namespace.yaml`
- Apply the role: `kubectl apply -f aflecha-role.yaml`
- Apply the service account: `kubectl apply -f aflecha-serviceaccount.yaml`
- Apply the role binding: `kubectl apply -f aflecha-rolebinding.yaml`

## Deploy
- Apply the statefulset: `kubectl apply -f aflecha-statefulset.yaml`