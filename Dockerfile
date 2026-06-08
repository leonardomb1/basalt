# syntax=docker/dockerfile:1

# basalt ships as a single static binary. Pipeline (.bsl) scripts are DATA —
# mounted at runtime (volume / ConfigMap / git-sync), never baked into the image,
# so editing a pipeline never means rebuilding the image.
#
#   batch:  docker run --rm -v "$PWD/pipelines:/scripts:ro" \
#             -e SR_USER -e SR_PASS  IMAGE run /scripts/etl.bsl
#   serve:  docker run --rm -p 8080:8080 -v "$PWD/pipelines:/scripts:ro" \
#             -e SR_USER -e SR_PASS  IMAGE serve /scripts
#   stdin:  cat etl.bsl | docker run --rm -i IMAGE run -

# --- build: compile a static musl binary ----------------------------------
FROM alpine:3.20 AS build
ARG ZIG_VERSION=0.15.2
RUN apk add --no-cache curl xz
# Zig's 0.14.1+ tarball naming is arch-first (zig-<arch>-<os>-<version>).
RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
      | tar -xJ -C /opt \
 && ln -s "/opt/zig-x86_64-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig
WORKDIR /src
COPY build.zig build.zig.zon ./
COPY src ./src
# Static musl link so the binary runs in a scratch/distroless image; -Dstrip drops
# debug info for a smaller release binary.
RUN zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl -Dstrip=true

# --- runtime: just the binary (ca-certs, nonroot, no shell) ---------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/zig-out/bin/basalt /usr/local/bin/basalt
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/basalt"]
CMD ["help"]
