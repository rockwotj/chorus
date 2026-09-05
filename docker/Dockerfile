FROM eclipse-temurin:11-jdk-jammy AS verifier

# The published v0.9.5 zip predates PVerifier's datatype and -M support.
# Build immutable source revisions and use matching Java/native Z3 bindings.
ARG UCLID_COMMIT=a4e4e7a22780833c86d6a650af2fcf88a7d00a06
ARG Z3_COMMIT=e417f7d78509b2d0c9ebc911fee7632e6ef546b6
ARG SBT_LAUNCH_SHA256=6581531c0188aef375b1a80eca877643d9fcae316299e0a67761d4c0ad7473e2
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates curl git python3 unzip \
    && rm -rf /var/lib/apt/lists/*
RUN git init /tmp/z3 \
    && git -C /tmp/z3 fetch --depth 1 https://github.com/Z3Prover/z3.git "${Z3_COMMIT}" \
    && git -C /tmp/z3 checkout --detach FETCH_HEAD \
    && cd /tmp/z3 && python3 scripts/mk_make.py --java \
    && cd build && make -j2 \
    && mkdir /opt/z3 \
    && cp z3 libz3.so libz3java.so com.microsoft.z3.jar /opt/z3/
RUN git init /tmp/uclid \
    && git -C /tmp/uclid fetch --depth 1 https://github.com/uclid-org/uclid.git "${UCLID_COMMIT}" \
    && git -C /tmp/uclid checkout --detach FETCH_HEAD \
    && cp /opt/z3/com.microsoft.z3.jar /tmp/uclid/lib/com.microsoft.z3.jar \
    && curl -fL --retry 5 -o /tmp/sbt-launch.jar \
       https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.4.9/sbt-launch-1.4.9.jar \
    && echo "${SBT_LAUNCH_SHA256}  /tmp/sbt-launch.jar" | sha256sum --check \
    && cd /tmp/uclid && java -jar /tmp/sbt-launch.jar universal:packageBin \
    && unzip -q target/universal/uclid-0.9.5.zip -d /opt \
    && mv /opt/uclid-0.9.5 /opt/uclid

FROM mcr.microsoft.com/dotnet/sdk:8.0-jammy

ARG P_VERSION=3.1.0

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl jq maven openjdk-17-jdk-headless pkg-config \
       python3 unzip \
    && rm -rf /var/lib/apt/lists/*

RUN dotnet tool install --tool-path /opt/p-tools P --version "${P_VERSION}"

COPY --from=verifier /opt/uclid /opt/uclid
COPY --from=verifier /opt/z3 /opt/z3

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none

ENV PATH=/root/.cargo/bin:/opt/p-tools:/opt/uclid/bin:/opt/z3:${PATH}
ENV LD_LIBRARY_PATH=/opt/z3
ENV JAVA_OPTS=-Djava.library.path=/opt/z3
WORKDIR /workspace

# rust-toolchain.toml is the one toolchain pin, shared with CI and local
# builds. Copied alone first so the toolchain layer survives source edits.
COPY rust-toolchain.toml ./
RUN rustup toolchain install

COPY . .
RUN p --version \
    && z3 --version \
    && uclid --help 2>&1 | grep -q 'mod-set-analysis' \
    && cargo fetch --manifest-path rust/Cargo.toml \
    && mvn -q -f p/pobserve/pom.xml dependency:go-offline \
    && cd p \
    && p compile -pp QuorumModel.pproj -md pobserve \
    && mvn -q -f pobserve/pom.xml package

CMD ["python3", "docker/scripts/orchestrate.py", "pipeline"]
