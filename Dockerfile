FROM registry.cn-hangzhou.aliyuncs.com/migo-dl/python:3.10.18-poetry-0-4-1-arm

# 设置工作目录
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gobject-introspection \
        libgirepository1.0-dev \
        libcairo2-dev \
        pkg-config \
        python3-dev \
        gir1.2-gstreamer-1.0 \
        gir1.2-gst-plugins-base-1.0 \
        gstreamer1.0-tools \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        libgstreamer1.0-0 \
        libgstreamer-plugins-base1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# 拷贝必要的文件以安装依赖
COPY pyproject.toml poetry.lock README.md ./
RUN mkdir -p src/artemis_cam && \
    touch src/artemis_cam/__init__.py && \
    poetry install --no-root

# 拷贝源代码文件
COPY . .

# 安装当前包
RUN poetry install

# 暴露 gRPC 服务端口
EXPOSE 50051

# 默认入口
CMD ["poetry", "run", "python", "-m", "artemis_cam.commands.app"]
