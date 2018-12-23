ARG BASE_IMAGE=gpdb-dev:centos6

FROM pivotaldata/${BASE_IMAGE}

# install Go utilities
RUN cd /tmp && \
    GO_VERSION=$(wget -qO- https://golang.org/dl/ | grep -E -o 'go1\.[0-9]+\.[0-9]+' | \
                 tr -d "go" | sort -V | uniq | tail -1) && \
    echo "Using GO version ${GO_VERSION}" && \
    wget -q https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm -f go${GO_VERSION}.linux-amd64.tar.gz && \
    cd - && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/golang/dep/cmd/dep && \
    GOPATH=/opt/go /usr/local/go/bin/go get github.com/onsi/ginkgo/ginkgo && \
    echo >> /etc/bashrc 'export GOPATH=/opt/go' && \
    echo >> /etc/bashrc 'export PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH'

ADD pxf_src /tmp/pxf_src

RUN cd /tmp/pxf_src && \
    export BUILD_PARAMS='-Dorg.gradle.daemon=false' && \
    GOPATH=/opt/go PATH=/opt/go/bin:/usr/local/go/bin:$PATH make tar && \
    cd automation && PXF_HOME=/tmp/pxf_src/server/build/stage make dev && \
    mkdir -p /home/gpadmin && \
    mv /root/.m2 /home/gpadmin && \
    mv /root/.gradle /home/gpadmin && \
    ln -s /home/gpadmin/.gradle /root/.gradle && \
    ln -s /home/gpadmin/.m2 /root/.m2 && \
    chown -R 1000:1000 /home/gpadmin && \
    rm -rf /tmp/pxf_src
