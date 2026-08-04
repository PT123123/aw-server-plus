#!/bin/bash
# Based on https://mozilla.github.io/firefox-browser-architecture/experiments/2017-09-21-rust-on-android.html
# Depended on by aw-android/scripts/setup-rust-with-ndk.sh
set -x;
set -e;
set -x;

#!/usr/bin/env bash
# 适配zsh/bash，统一使用bash解释器执行（避免Shell兼容问题）

# 核心配置（和原有脚本保持一致）
source $HOME/.cargo/env
NDK_VERSION=r25c
NDK_BUILD_VERSION=25.2.9519653  # r25c对应的官方完整版本号
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
project_path="$(readlink -f "$script_dir/.")"
platform="$(uname -s | tr '[:upper:]' '[:lower:]')"

# ====================== 核心判断：ANDROID_NDK_HOME 是否已存在 ======================
if [ -z "$ANDROID_NDK_HOME" ]; then
    # 1. 检查项目根目录是否有本地NDK文件夹（兼容原有逻辑）
    if [ -d "$(pwd)/NDK" ]; then
        echo "✅ Found NDK folder in root, using."
        ANDROID_NDK_HOME="$(pwd)/NDK"
    else
        # 2. 检查是否已通过sdkmanager安装NDK（Google官方路径）
        SDK_NDK_PATH=~/Android/Sdk/ndk/$NDK_BUILD_VERSION
        if [ -d "$SDK_NDK_PATH" ]; then
            echo "✅ Found NDK installed by sdkmanager, using: $SDK_NDK_PATH"
            ANDROID_NDK_HOME=$SDK_NDK_PATH
        else
            # 3. 未安装则自动用sdkmanager安装（Google最新规范）
            echo "⚠️ ANDROID_NDK_HOME not set, installing NDK via sdkmanager (Google official way)..."
            
            # 安装Java 17（sdkmanager必需）
            if ! java -version 2>&1 | grep -q "17."; then
                echo "🔧 Installing Java 17 (required for sdkmanager)..."
                sudo apt update && sudo apt install -y openjdk-17-jdk
                sudo update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java
            fi

            # 下载并配置cmdline-tools（sdkmanager依赖）
            echo "🔧 Downloading Android cmdline-tools..."
            mkdir -p ~/Android/Sdk/cmdline-tools
            cd ~/Android/Sdk
            rm -rf cmdline-tools/latest  # 清理旧目录避免冲突
            wget -c https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip -O cmdline-tools.zip
            unzip -q cmdline-tools.zip -d cmdline-tools
            mv cmdline-tools/cmdline-tools cmdline-tools/latest
            rm -f cmdline-tools.zip

            # 配置Shell环境变量（自动识别zsh/bash）
            SHELL_CONF=$(if [ "$SHELL" = "/bin/zsh" ]; then echo ~/.zshrc; elif [ "$SHELL" = "/bin/bash" ]; then echo ~/.bashrc; else echo ~/.profile; fi)
            echo "export ANDROID_HOME=~/Android/Sdk" >> $SHELL_CONF
            echo "export PATH=\$ANDROID_HOME/cmdline-tools/latest/bin:\$PATH" >> $SHELL_CONF
            source $SHELL_CONF

            # 接受SDK授权并安装NDK
            echo "🔧 Accepting SDK licenses..."
            yes | sdkmanager --licenses
            
            echo "🔧 Installing NDK $NDK_VERSION ($NDK_BUILD_VERSION)..."
            sdkmanager "ndk;$NDK_BUILD_VERSION"

            # 设置ANDROID_NDK_HOME为官方安装路径
            ANDROID_NDK_HOME=$SDK_NDK_PATH
            echo "export ANDROID_NDK_HOME=$ANDROID_NDK_HOME" >> $SHELL_CONF
            source $SHELL_CONF
        fi
    fi
else
    echo "✅ ANDROID_NDK_HOME already set: $ANDROID_NDK_HOME"
fi

# ====================== 原有脚本的后续逻辑（完全保留，适配新NDK路径） ======================
# Needed since dependency 'ring' doesn't respect .cargo/config
echo "🔧 Setting up toolchain binary symlinks..."
NDK_TOOLCHAIN_BIN=$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin

# 兼容官方NDK路径的符号链接（修复原有mv逻辑的路径问题）
if [ ! -d "$NDK_TOOLCHAIN_BIN" ]; then
    echo "⚠️ NDK toolchain path not found, fallback to legacy path..."
    NDK_TOOLCHAIN_BIN=$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin
fi

for arch in 'aarch64' 'x86_64' 'i686'; do
    ln -s -f $NDK_TOOLCHAIN_BIN/$arch-linux-android26-clang $NDK_TOOLCHAIN_BIN/$arch-linux-android-clang
done

# This has a slightly different path from the ones above
ln -s -f $NDK_TOOLCHAIN_BIN/armv7a-linux-androideabi26-clang $NDK_TOOLCHAIN_BIN/armv7a-linux-androideabi-clang
ln -s -f $NDK_TOOLCHAIN_BIN/armv7a-linux-androideabi26-clang $NDK_TOOLCHAIN_BIN/arm-linux-androideabi-clang

# Add to Rust
export RUSTUP_DIST_SERVER=https://rsproxy.cn
export RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
echo "🔧 Setting up Rust toolchains..."
rustup target add aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android;

# Creates cargo config
echo "🔧 Creating cargo config..."
mkdir -p $project_path/.cargo
cat > $project_path/.cargo/config << EOF
[target.aarch64-linux-android]
ar = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/llvm-ar'
linker = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/aarch64-linux-android26-clang'

[target.armv7-linux-androideabi]
ar = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/llvm-ar'
linker = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/armv7a-linux-androideabi-clang'

[target.i686-linux-android]
ar = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/llvm-ar'
linker = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/i686-linux-android26-clang'

[target.x86_64-linux-android]
ar = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/llvm-ar'
linker = '$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$platform-x86_64/bin/x86_64-linux-android26-clang'
EOF

echo "✅ All setup completed successfully! ANDROID_NDK_HOME: $ANDROID_NDK_HOME"
