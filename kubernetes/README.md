# Kubernetes resource files

This directory contains the Kubernetes resource files for deploying the application.

## Setup
- Apply the namespace: `kubectl apply -f aflecha-namespace.yaml`
- Apply the role: `kubectl apply -f aflecha-role.yaml`
- Apply the service account: `kubectl apply -f aflecha-serviceaccount.yaml`
- Apply the role binding: `kubectl apply -f aflecha-rolebinding.yaml`

## Deploy
- Apply the statefulset: `kubectl apply -f aflecha-statefulset.yaml`