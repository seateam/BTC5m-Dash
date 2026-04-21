#!/bin/bash
set -e

cd "$(dirname "$0")"

# 检查 .env 是否存在
if [ ! -f .env ]; then
  echo "未找到 .env 文件，请先复制 .env.example 并填写配置："
  echo "  cp .env.example .env"
  echo "  然后编辑 .env 填入 POLYMARKET_PRIVATE_KEY 和 POLYMARKET_PROXY_ADDRESS"
  exit 1
fi

# 自动安装 Node.js（仅 Linux，macOS 通常已装）
if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  if [ "$(uname -s)" = "Linux" ]; then
    echo "未检测到 Node.js，正在自动安装 Node.js 20..."
    if ! command -v curl >/dev/null 2>&1; then
      sudo apt-get update && sudo apt-get install -y curl
    fi
    if command -v apt-get >/dev/null 2>&1; then
      curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
      sudo apt-get install -y nodejs
    elif command -v yum >/dev/null 2>&1; then
      curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
      sudo yum install -y nodejs
    else
      echo "无法识别包管理器，请手动安装 Node.js 20+ 后重试"
      exit 1
    fi
    echo "Node.js 安装完成：$(node -v)"
  else
    echo "未找到 Node.js 或 npm，请先安装 Node.js 20+（访问 https://nodejs.org）"
    exit 1
  fi
fi

# 安装依赖
if [ ! -d node_modules ]; then
  echo "正在安装依赖..."
  npm install
fi

# 如果端口已被占用，先杀掉旧进程
if lsof -ti:3456 > /dev/null 2>&1; then
  echo "端口 3456 已被占用，正在关闭旧进程..."
  lsof -ti:3456 | xargs kill -9
  sleep 1
fi

echo "启动 BTC 5m 盘口监控..."
APP_MODE=$(grep -E '^APP_MODE=' .env | tail -n1 | cut -d= -f2 | tr -d '\r' | tr -d '"')
if [ -z "$APP_MODE" ]; then
  APP_MODE="full"
fi
echo "运行模式: $APP_MODE"
echo "状态接口: http://localhost:3456/api/state"
if [ "$APP_MODE" != "headless" ]; then
  echo "浏览器地址: http://localhost:3456"
fi
echo "按 Ctrl+C 退出"
echo ""

# 退出时清理所有子进程
trap 'kill -- -$$ 2>/dev/null; exit 0' INT TERM EXIT

# 等服务就绪后自动打开浏览器
if [ "$APP_MODE" != "headless" ] && [ "$(uname -s)" = "Darwin" ]; then
  (
    sleep 2
    URL="http://localhost:3456"
    open "$URL" >/dev/null 2>&1 || true
  ) &
fi

npx tsx server.ts
