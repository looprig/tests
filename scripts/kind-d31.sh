#!/bin/sh
# D3.1 disposable-namespace lane: a local kind cluster (1 control-plane + 1
# worker) with a local registry, and the lane's two product binaries baked
# into images and pushed there.
#
#   scripts/kind-d31.sh up      registry + cluster
#   scripts/kind-d31.sh images  build, push; prints KIND_HOST_IMAGE/KIND_CONTROL_IMAGE
#   scripts/kind-d31.sh down    delete the cluster and the registry
#
# Then, with the two printed variables exported:
#   LOOPRIG_KIND=1 GOWORK=off go test -tags 'integration kind' -run '^TestDedicated' -v -timeout 30m .
#
# The registry exists because the controller refuses an image that is not
# pinned by digest, and an image `kind load`-ed has no registry digest a Pod
# could name; a pushed image does.
set -eu
CLUSTER=${KIND_CLUSTER:-looprig-d31}
REGISTRY=${KIND_REGISTRY:-looprig-d31-registry}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${KIND_OUT:-$ROOT/.kind-d31}
ARCH=$(docker version -f '{{.Server.Arch}}')

case "${1:-}" in
up)
	docker inspect "$REGISTRY" >/dev/null 2>&1 ||
		docker run -d --restart=no -p 127.0.0.1:5001:5000 --name "$REGISTRY" registry:2 >/dev/null
	config=$(mktemp)
	# API-server audit: every Pod delete/update at Request level (the body
	# carries DeleteOptions.preconditions.uid), written on the control-plane
	# node at /var/log/kubernetes/audit.log. The lane reads it (D3.1 F3).
	auditdir=$(mktemp -d)
	cat >"$auditdir/policy.yaml" <<YAML
apiVersion: audit.k8s.io/v1
kind: Policy
omitStages: [RequestReceived]
rules:
  - level: Request
    verbs: [delete, update]
    resources: [{group: "", resources: [pods]}]
  - level: None
YAML
	cat >"$config" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: $CLUSTER
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
nodes:
  - role: control-plane
    extraMounts:
      - {hostPath: "$auditdir/policy.yaml", containerPath: /etc/kubernetes/audit/policy.yaml, readOnly: true}
    kubeadmConfigPatches:
      - |
        kind: ClusterConfiguration
        apiServer:
          extraArgs:
            audit-policy-file: /etc/kubernetes/audit/policy.yaml
            audit-log-path: /var/log/kubernetes/audit.log
            audit-log-maxsize: "100"
          extraVolumes:
            - {name: audit-policy, hostPath: /etc/kubernetes/audit, mountPath: /etc/kubernetes/audit, readOnly: true, pathType: DirectoryOrCreate}
            - {name: audit-log, hostPath: /var/log/kubernetes, mountPath: /var/log/kubernetes, readOnly: false, pathType: DirectoryOrCreate}
  - role: worker
    extraPortMappings:
      - {containerPort: 30422, hostPort: 30422, listenAddress: "127.0.0.1"}
      - {containerPort: 30480, hostPort: 30480, listenAddress: "127.0.0.1"}
YAML
	kind create cluster --config "$config"
	rm -f "$config"
	docker network connect kind "$REGISTRY" 2>/dev/null || true
	for node in $(kind get nodes --name "$CLUSTER"); do
		docker exec "$node" sh -c "mkdir -p /etc/containerd/certs.d/localhost:5001 &&
			printf '[host.\"http://$REGISTRY:5000\"]\n' >/etc/containerd/certs.d/localhost:5001/hosts.toml"
	done
	;;
images)
	mkdir -p "$OUT"
	for bin in kindhost kindcontrol; do
		(cd "$ROOT" && GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
			go build -trimpath -tags 'integration kind' -o "$OUT/$bin" "./internal/kindlane/cmd/$bin")
		docker build -q -t "localhost:5001/looprig-d31/$bin:dev" --build-arg BIN="$bin" \
			-f "$ROOT/internal/kindlane/Dockerfile" "$OUT" >/dev/null
		docker push -q "localhost:5001/looprig-d31/$bin:dev" >/dev/null
		digest=$(curl -fsS -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
			-o /dev/null -w '%header{docker-content-digest}' "http://localhost:5001/v2/looprig-d31/$bin/manifests/dev")
		var=KIND_HOST_IMAGE
		[ "$bin" = kindcontrol ] && var=KIND_CONTROL_IMAGE
		echo "export $var=localhost:5001/looprig-d31/$bin@$digest"
	done
	;;
down)
	kind delete cluster --name "$CLUSTER"
	docker rm -f "$REGISTRY" >/dev/null 2>&1 || true
	;;
*)
	echo "usage: $0 up|images|down" >&2
	exit 2
	;;
esac
