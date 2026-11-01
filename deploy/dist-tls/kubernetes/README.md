# Distribution over mutual TLS on Kubernetes

A recipe for the reference manifests in `deploy/kubernetes/`: every controller pod gets its
own certificate from a distribution authority that cert-manager keeps, naming the pod's IP,
and runs Erlang distribution with `deploy/dist-tls/dist_tls.conf`. The default manifests do
not need cert-manager; this recipe is applied on top of them.

```sh
$ kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
$ kubectl -n cert-manager rollout status deploy/cert-manager-webhook
$ kubectl apply -f deploy/dist-tls/kubernetes/issuer.yaml
$ kubectl -n flezha create configmap flezha-dist-tls --from-file=deploy/dist-tls/dist_tls.conf
$ kubectl -n flezha patch daemonset flezha-daemonset --patch-file deploy/dist-tls/kubernetes/daemonset-patch.yaml
```

- `issuer.yaml`: the authority (a self-signed root in the Secret `flezha-dist-ca`), the Issuer
  that signs with it, and the right for the controller's service account to request its
  certificate.
- `daemonset-patch.yaml`: two init containers that make the pod's key and have its
  certificate signed, a memory volume at `/etc/flezha/dist` for the three files, and
  `FLEZHA_DIST_TLS_OPTFILE`.

The console joins the same way: give its pod the two init containers and the volume, and set
`FLEZHA_DIST_TLS_OPTFILE` for it. `test/scripts/dist_hardening_k3s.sh` applies this recipe to
a throwaway cluster and checks it. The trust model behind it is in
`docs/public/topics/core/authorization.md`.
