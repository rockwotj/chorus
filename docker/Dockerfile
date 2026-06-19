FROM mcr.microsoft.com/dotnet/sdk:8.0-jammy

ARG P_VERSION=3.1.0
ARG UCLID_VERSION=0.9.5
ARG UCLID_SHA256=57dfe2eebbbd9111d9cbf87dc2d8ce4d11a3f2390c5a97f2ba5217d5ac8aa23a

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl jq maven openjdk-17-jdk-headless pkg-config \
       python3 unzip z3 \
    && rm -rf /var/lib/apt/lists/*

RUN dotnet tool install --tool-path /opt/p-tools P --version "${P_VERSION}"

RUN curl -L --fail --retry 5 \
      -o /tmp/uclid.zip \
      "https://github.com/uclid-org/uclid/releases/download/v${UCLID_VERSION}/uclid-${UCLID_VERSION}.zip" \
    && echo "${UCLID_SHA256}  /tmp/uclid.zip" | sha256sum --check \
    && unzip -q /tmp/uclid.zip -d /opt \
    && mv "/opt/uclid-${UCLID_VERSION}" /opt/uclid \
    && rm /tmp/uclid.zip

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none

ENV PATH=/root/.cargo/bin:/opt/p-tools:/opt/uclid/bin:${PATH}
WORKDIR /workspace

# rust-toolchain.toml is the one toolchain pin, shared with CI and local
# builds. Copied alone first so the toolchain layer survives source edits.
COPY rust-toolchain.toml ./
RUN rustup toolchain install

COPY . .
RUN p --version \
    && z3 --version \
    && uclid 2>&1 | grep -q 'uclid 0.9.5' \
    && cargo fetch --manifest-path rust/Cargo.toml \
    && mvn -q -f p/pobserve/pom.xml dependency:go-offline \
    && cd p \
    && p compile -pp QuorumModel.pproj -md pobserve \
    && mvn -q -f pobserve/pom.xml package

CMD ["python3", "docker/scripts/orchestrate.py", "pipeline"]
