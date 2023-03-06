FROM ubuntu:20.04 AS base

USER root
WORKDIR /

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends vim \
    pip \
    curl \
    wget \
    systemctl \
    openssh-server \
    flex \
    libfl-dev \
    bison \
    unzip \
    zip \
    ca-certificates \
    python3-sphinx \
    nasm \
    pkg-config && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common && \
    apt-get update && \
    add-apt-repository ppa:git-core/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Dependencies for presto + velox
COPY ./scripts-presto /scripts-presto

RUN pushd scripts-presto && \
    pushd velox && \
    sed -i 's/sudo //g' scripts/setup-ubuntu.sh && \
    sed -i '/git \\/d' scripts/setup-ubuntu.sh && \
    chmod +x scripts/setup-ubuntu.sh && \
    popd && \
    sed -i 's/sudo //g' scripts/setup-ubuntu.sh && \
    chmod +x scripts/setup-ubuntu.sh && \
    apt update && \
    ./scripts/setup-ubuntu.sh && \
    popd && \
    rm -rf /scripts-presto && \
    rm -rf /var/lib/apt/lists/*

# Dependencies for cider
COPY ./scripts-cider /scripts-cider

RUN pushd scripts-cider && \
    sed -i 's/sudo //g' common-functions.sh && \
    sed -i 's/sudo //g' cider-deps-ubuntu.sh && \
    sed -i '/git \\/d' cider-deps-ubuntu.sh && \
    sed -i 's/install_folly/# install_folly/g' cider-deps-ubuntu.sh && \
    ./cider-deps-ubuntu.sh --nocuda && \
    popd && \
    rm -rf /scripts-cider && \
    rm -rf /var/lib/apt/lists/*

# Dependencies for CI
RUN pip install regex \
    black \
    cmakelang && \
    rm -rf /var/lib/apt/lists/*

# GraalVM jdk17 22.0.0.2
RUN mkdir /graalvm && \
    curl -fsSL "https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-22.0.0.2/graalvm-ce-java17-linux-amd64-22.0.0.2.tar.gz" \
        | tar -zxC /graalvm --strip-components 1 && \
    find /graalvm -name "*src.zip"  -printf "Deleting %p\n" -exec rm {} + && \
    { test ! -d /graalvm/legal || tar czf /graalvm/legal.tgz /graalvm/legal/; } && \
    { test ! -d /graalvm/legal || rm -r /graalvm/legal; } && \
    rm -rf /graalvm/man `# does not exist in java11 package` && \
    echo Cleaning up... && \
    echo 'PATH="/graalvm/bin:$PATH"' | install --mode 0644 /dev/stdin /etc/profile.d/graal-on-path.sh && \
    echo OK

ENV PATH=/graalvm/bin:$PATH

# install Gradle and native-image
RUN curl -s "https://get.sdkman.io" | bash && \
    /bin/bash -c "source /root/.sdkman/bin/sdkman-init.sh && sdk install gradle " && \
    gu install native-image

# setup gradle proxy since gradle do not use system proxy.
# Note: Please set your own gradle proxy when run this image
ARG HttpProxyHost
ARG HttpsProxyHost
ARG HttpProxyPort
ARG HttpsProxyPort

RUN mkdir -p ~/.gradle/ && \
    touch ~/.gradle/gradle.properties && \
    echo "systemProp.http.proxyHost=${HttpProxyHost}" >> ~/.gradle/gradle.properties && \
    echo "systemProp.https.proxyHost=${HttpsProxyHost}" >> ~/.gradle/gradle.properties && \
    echo "systemProp.https.proxyPort=${HttpProxyPort}" >> ~/.gradle/gradle.properties && \
    echo "systemProp.http.proxyPort=${HttpsProxyPort}" >> ~/.gradle/gradle.properties

# build isthmus
# 63cfcb33fa7818e91303d79317c12b8489a9014f is at main branch on 20230129
# @link: https://github.com/substrait-io/substrait-java/commit/63cfcb33fa7818e91303d79317c12b8489a9014f
RUN git clone https://github.com/substrait-io/substrait-java /substrait-java && \
    pushd /substrait-java && \
    git checkout 63cfcb33fa7818e91303d79317c12b8489a9014f && \
    git submodule update --init --recursive && \
    /bin/bash -c "source /root/.sdkman/bin/sdkman-init.sh && gradle nativeImage --info" && \
    popd

# Remove gradle proxy
RUN rm ~/.gradle/gradle.properties

ENV ISTHMUS_EXEC=/substrait-java/isthmus/build/graal/isthmus
