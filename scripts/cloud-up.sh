#!/bin/sh
# cloud-up.sh starts the LOCAL stand-ins for the cloud composition lane
# (runbook 07 task P3.1): one PostgreSQL, one PgBouncer in transaction mode in
# front of it, and one MinIO serving S3 over TLS with a static KMS key. Nothing
# here contacts a real cloud. Every image is pinned by digest, so a moved tag
# cannot change what the lane verifies.
#
# It writes an environment file the cloud-tagged tests read, and prints its
# path. Source it, run the lane, then run cloud-down.sh:
#
#   . "$(sh scripts/cloud-up.sh)"
#   LOOPRIG_CLOUD=1 GOWORK=off go test -tags 'integration cloud' -run '^TestCloud' .
#   sh scripts/cloud-down.sh
#
# The containers, network and scratch directory are all named from
# LOOPRIG_CLOUD_NAME (default looprig-p31) so a second copy can run beside
# this one by choosing another name and other ports.
set -eu

NAME="${LOOPRIG_CLOUD_NAME:-looprig-p31}"
PG_PORT="${LOOPRIG_CLOUD_PG_PORT:-55432}"
BOUNCER_PORT="${LOOPRIG_CLOUD_BOUNCER_PORT:-56433}"
S3_PORT="${LOOPRIG_CLOUD_S3_PORT:-59000}"
SCRATCH="${LOOPRIG_CLOUD_SCRATCH:-${TMPDIR:-/tmp}/${NAME}-cloud}"

# Pinned by digest. postgres 17 is inside pgstore's verified range (its README
# names 14-18); the PgBouncer image and configuration are the ones pgstore's
# docs/OPERATIONS.md measured; MinIO is the last community release published
# as an image.
POSTGRES_IMAGE="postgres:17.11@sha256:67f41722b7a8cbdb868a44a4995c846eddfdc2973bccb291ce937dce88ad5675"
BOUNCER_IMAGE="edoburu/pgbouncer:v1.25.2-p0@sha256:7d7a27d9e90985cab5cf42256f5c13a3120baa4b055b69df37beb272b89b2340"
MINIO_IMAGE="minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"

PG_PASSWORD="$(openssl rand -hex 16)"
S3_ACCESS="$(openssl rand -hex 10)"
S3_SECRET="$(openssl rand -hex 20)"
KMS_KEY_NAME="looprig-p31-key"
KMS_KEY="$(openssl rand -base64 32)"
BUCKET="looprig-cloud"

rm -rf "$SCRATCH"
mkdir -p "$SCRATCH/certs"
chmod 700 "$SCRATCH"

# A self-signed loopback certificate. s3store refuses plain HTTP to anything
# but an explicitly-allowed loopback endpoint, and its cloud encryption test
# does not allow it, so MinIO serves TLS and the AWS SDK trusts this file
# through AWS_CA_BUNDLE.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
	-subj "/CN=127.0.0.1" \
	-addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
	-keyout "$SCRATCH/certs/private.key" -out "$SCRATCH/certs/public.crt" >/dev/null 2>&1
chmod 644 "$SCRATCH/certs/private.key" "$SCRATCH/certs/public.crt"

docker network create "$NAME-net" >/dev/null

docker run -d --name "$NAME-pg" --network "$NAME-net" \
	-p "127.0.0.1:$PG_PORT:5432" \
	-e POSTGRES_PASSWORD="$PG_PASSWORD" \
	"$POSTGRES_IMAGE" \
	-c shared_preload_libraries=pg_stat_statements \
	-c pg_stat_statements.track=all \
	-c max_connections=300 >/dev/null

docker run -d --name "$NAME-bouncer" --network "$NAME-net" \
	-p "127.0.0.1:$BOUNCER_PORT:5432" \
	-e DB_HOST="$NAME-pg" -e DB_USER=postgres -e DB_PASSWORD="$PG_PASSWORD" -e DB_NAME=postgres \
	-e POOL_MODE=transaction -e AUTH_TYPE=scram-sha-256 -e ADMIN_USERS=postgres \
	-e MAX_CLIENT_CONN=400 -e DEFAULT_POOL_SIZE=20 \
	-e SERVER_RESET_QUERY="DISCARD ALL" \
	-e IGNORE_STARTUP_PARAMETERS=extra_float_digits \
	"$BOUNCER_IMAGE" >/dev/null

docker run -d --name "$NAME-s3" --network "$NAME-net" \
	-p "127.0.0.1:$S3_PORT:9000" \
	-v "$SCRATCH/certs:/certs:ro" \
	-e MINIO_ROOT_USER="$S3_ACCESS" -e MINIO_ROOT_PASSWORD="$S3_SECRET" \
	-e MINIO_KMS_SECRET_KEY="$KMS_KEY_NAME:$KMS_KEY" \
	"$MINIO_IMAGE" server /data --certs-dir /certs >/dev/null

i=0
until docker exec "$NAME-pg" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1; do
	i=$((i + 1)); [ "$i" -lt 60 ] || { echo "postgres did not become ready" >&2; exit 1; }
	sleep 1
done
# pg_isready answers during the entrypoint's init restart; wait for a query.
i=0
until docker exec "$NAME-pg" psql -U postgres -h 127.0.0.1 -Atc 'select 1' >/dev/null 2>&1; do
	i=$((i + 1)); [ "$i" -lt 60 ] || { echo "postgres did not accept queries" >&2; exit 1; }
	sleep 1
done
docker exec "$NAME-pg" psql -U postgres -h 127.0.0.1 -qc 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' >/dev/null

i=0
until docker exec "$NAME-s3" mc --insecure alias set local https://127.0.0.1:9000 "$S3_ACCESS" "$S3_SECRET" >/dev/null 2>&1; do
	i=$((i + 1)); [ "$i" -lt 60 ] || { echo "minio did not become ready" >&2; exit 1; }
	sleep 1
done
docker exec "$NAME-s3" mc --insecure mb "local/$BUCKET" >/dev/null
# The bucket DEFAULT is SSE-S3, so the bucket-default leg of s3store's cloud
# encryption test has a policy to observe.
docker exec "$NAME-s3" mc --insecure encrypt set sse-s3 "local/$BUCKET" >/dev/null

ENV_FILE="$SCRATCH/cloud.env"
umask 077
cat >"$ENV_FILE" <<EOF
export LOOPRIG_CLOUD=1
export LOOPRIG_CLOUD_NAME='$NAME'
export LOOPRIG_CLOUD_PG_CONTAINER='$NAME-pg'
export LOOPRIG_CLOUD_BOUNCER_CONTAINER='$NAME-bouncer'
export LOOPRIG_CLOUD_S3_CONTAINER='$NAME-s3'
export LOOPRIG_CLOUD_PG_DSN='postgres://postgres:$PG_PASSWORD@127.0.0.1:$PG_PORT/postgres?sslmode=disable'
export LOOPRIG_CLOUD_BOUNCER_DSN='postgres://postgres:$PG_PASSWORD@127.0.0.1:$BOUNCER_PORT/postgres?sslmode=disable'
export LOOPRIG_CLOUD_S3_ENDPOINT='https://127.0.0.1:$S3_PORT'
export LOOPRIG_CLOUD_S3_BUCKET='$BUCKET'
export LOOPRIG_CLOUD_S3_KMS_KEY_ID='$KMS_KEY_NAME'
export AWS_ACCESS_KEY_ID='$S3_ACCESS'
export AWS_SECRET_ACCESS_KEY='$S3_SECRET'
export AWS_REGION='us-east-1'
export AWS_CA_BUNDLE='$SCRATCH/certs/public.crt'
export AWS_EC2_METADATA_DISABLED=true
# s3store's own cloud-tagged encryption test reads these.
export S3STORE_CLOUD_ENDPOINT='https://127.0.0.1:$S3_PORT'
export S3STORE_CLOUD_BUCKET='$BUCKET'
export S3STORE_CLOUD_REGION='us-east-1'
export S3STORE_CLOUD_PREFIX='s3store-cloud-check'
export S3STORE_CLOUD_KMS_KEY_ID='$KMS_KEY_NAME'
EOF
echo "$ENV_FILE"
