# operator — AI Assistant Context

Rust controller for ZooNetwork / ZooChain / ZooExplorer / ZooGateway.
Deployed from `zooai/universe` `k8s/base/operator.yaml`; one replica set in
`zoo-system` watches every namespace, all other overlays scale to 0.

## Health endpoints — do not re-braid
`/healthz` and `/readyz` (port 8081) both report **process health only**.
Readiness deliberately does not consult the lease: gating it on leadership
leaves every non-leader NotReady forever, so the Deployment never goes
available. `kubectl get lease zoo-operator-leader -n zoo-system` is the one
place that names the leader.

`OPERATOR_NAMESPACE` (fieldRef in the manifest) decides which namespace holds
that lease. Unset, `main.rs` falls back to the literal `"zoo-system"`, so a
replica in any namespace will write zoo-system's lease.

## Building an image
`.github/workflows/docker.yml` asks for `zoo-build-linux-amd64`. **No runner
carries that label** — the arcd pool on `spark` is arm64-only, and building
linux/amd64 there goes through QEMU (a run took 51 min and was cancelled).
Until the repo moves to the canonical `hanzo.yml` + `hanzoai/ci` path, cut
images on the in-cluster buildkit lane, which is amd64-native and takes ~3 min:

    kubectl --context do-sfo3-hanzo-k8s -n hanzo-build create job ... \
      moby/buildkit:v0.16.0-rootless buildctl-daemonless.sh build \
      --opt context=https://github.com/zooai/operator.git#<sha> \
      --secret id=GIT_AUTH_TOKEN,env=GIT_AUTH_TOKEN \
      --secret id=gh_token,env=GIT_AUTH_TOKEN \
      --output type=image,name=ghcr.io/zooai/operator:<x.y.z>,push=true

Both secret ids come from the same PAT (`console-git-token`): buildkit uses
`GIT_AUTH_TOKEN` for the private git context, the Dockerfile mounts `gh_token`
so cargo can fetch private `hanzoai/operator-core`. Push creds = `push-zooai`
mounted at `DOCKER_CONFIG=/ghcr`. Node pool `runner-pool=32g` is tainted
`dedicated=ci-runner`, so the Job needs that toleration.
