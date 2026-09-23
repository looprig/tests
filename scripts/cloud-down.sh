#!/bin/sh
# cloud-down.sh removes everything cloud-up.sh created: the three containers,
# their network, and the scratch directory holding the certificate and the
# environment file (which carries the generated credentials).
set -u

NAME="${LOOPRIG_CLOUD_NAME:-looprig-p31}"
SCRATCH="${LOOPRIG_CLOUD_SCRATCH:-${TMPDIR:-/tmp}/${NAME}-cloud}"

docker rm -f -v "$NAME-s3" "$NAME-bouncer" "$NAME-pg" >/dev/null 2>&1
docker network rm "$NAME-net" >/dev/null 2>&1
rm -rf "$SCRATCH"
exit 0
