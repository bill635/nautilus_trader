#!/bin/bash
# 币安延迟测试一键运行脚本

echo "🚀 币安Orderbook延迟测试工具"
echo "="*50

echo "选择测试版本:"
echo "1. CCXT轮询版本 (推荐，无需API密钥)"
echo "2. NautilusTrader版本 (需要API密钥)"
echo "3. 退出"

read -p "请选择 (1-3): " choice

case $choice in
    1)
        echo "🚀 启动CCXT轮询版本..."
        docker run --rm -it -v "$(pwd)/reports:/app/reports" binance-ccxt-polling
        ;;
    2)
        echo "🔑 启动NautilusTrader版本..."
        if [ -z "$BINANCE_API_KEY" ] || [ -z "$BINANCE_API_SECRET" ]; then
            echo "❌ 需要设置API凭证:"
            echo "export BINANCE_API_KEY=your_api_key"
            echo "export BINANCE_API_SECRET=your_api_secret"
            exit 1
        fi
        docker run --rm -it \
            -v "$(pwd)/reports:/app/reports" \
            -e BINANCE_API_KEY="$BINANCE_API_KEY" \
            -e BINANCE_API_SECRET="$BINANCE_API_SECRET" \
            binance-fixed-test
        ;;
    3)
        echo "👋 退出"
        exit 0
        ;;
    *)
        echo "❌ 无效选择"
        exit 1
        ;;
esac

echo "📊 查看结果:"
ls -la reports/ | tail -5
