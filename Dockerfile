FROM mcr.microsoft.com/cbl-mariner/base/core:2.0

WORKDIR /app

RUN tdnf update -y && \
    tdnf install -y \
        git \
        gcc \
        gobject-introspection-devel \
        cairo-gobject \
        cairo-devel \
        pkg-config \
        libvirt-devel \
        python3-devel \
        python3-pip \
        python3-virtualenv \
        build-essential \
        cairo-gobject-devel \
        curl \
        ca-certificates && \
    tdnf clean all && \
    rm -rf /var/cache/tdnf /tmp/*

RUN git clone --depth 1 --branch $(curl --silent "https://api.github.com/repos/microsoft/lisa/releases/latest" \
    | grep '"tag_name":' \
    | sed -E 's/.*"([^"]+)".*/\1/') https://github.com/microsoft/lisa.git /app/lisa

WORKDIR /app/lisa

RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir --editable .[azure,libvirt,baremetal] --config-settings editable_mode=compat

