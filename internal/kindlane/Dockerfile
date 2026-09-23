# D3.1 kind lane image. The binary is cross-compiled by scripts/kind-d31.sh
# (CGO_ENABLED=0, -tags 'integration kind') and copied in; the image carries
# nothing else. Numeric non-root user: the controller adapter's Pod spec sets
# runAsNonRoot, a read-only root filesystem and drops every capability.
FROM scratch
ARG BIN
COPY ${BIN} /app
USER 65532:65532
ENTRYPOINT ["/app"]
