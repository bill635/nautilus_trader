# NautilusTrader Docker 币安延迟测试完整指南

## 🐳 Docker安装指南

### macOS 安装 Docker

1. **下载Docker Desktop for Mac**
   - 访问: https://www.docker.com/products/docker-desktop/
   - 下载适合您Mac的版本（Intel或Apple Silicon）

2. **安装Docker Desktop**
   ```bash
   # 如果使用Homebrew（推荐）
   brew install --cask docker
   
   # 或者直接下载.dmg文件安装
   ```

3. **启动Docker Desktop**
   - 从Applications文件夹启动Docker Desktop
   - 等待Docker完全启动（状态栏图标变绿）

4. **验证安装**
   ```bash
   docker --version
   docker run hello-world
   ```

## 🚀 快速运行延迟测试

### 方法1: 一键运行脚本（推荐）

```bash
# 在nautilus_trader项目根目录下执行
./run-docker-latency-test.sh
```

这个脚本会自动：
- 检查Docker环境
- 构建测试镜像
- 运行延迟测试
- 保存结果到 `./reports/` 目录

### 方法2: 使用docker-compose

```bash
# 使用docker-compose运行
docker-compose -f docker-compose.latency.yml up --build
```

### 方法3: 手动Docker命令

```bash
# 1. 构建镜像
docker build -f Dockerfile.latency-test -t nautilus-latency-test .

# 2. 运行测试
docker run --rm -it \
    -v "$(pwd)/reports:/app/reports" \
    --name nautilus-binance-latency-test \
    nautilus-latency-test
```

## 📊 测试配置

- **测试币种**: API3, OM, CFX
- **市场类型**: 现货 + USDT永续合约
- **测试时长**: 1分钟
- **数据深度**: 5档orderbook
- **统计间隔**: 每10秒输出一次实时统计

## 📁 文件结构

```
nautilus_trader/
├── Dockerfile.latency-test          # 延迟测试专用Dockerfile
├── docker-compose.latency.yml       # Docker Compose配置
├── run-docker-latency-test.sh       # 一键运行脚本
├── docker/
│   ├── latency_test_nautilus.py     # 延迟测试Python脚本
│   └── docker_entrypoint.sh         # Docker入口脚本
└── reports/                         # 测试报告输出目录
    └── binance_latency_nautilus_*.txt
```

## 🔧 高级用法

### 自定义测试参数

修改 `docker/latency_test_nautilus.py` 中的配置：

```python
strategy_config = DockerBinanceLatencyConfig(
    test_symbols=["API3", "OM", "CFX"],      # 修改测试币种
    test_duration_minutes=1,                  # 修改测试时长
    orderbook_depth=5,                       # 修改orderbook深度
    stats_interval_seconds=10,               # 修改统计间隔
)
```

### 仅构建镜像

```bash
./run-docker-latency-test.sh --build-only
```

### 清理Docker资源

```bash
./run-docker-latency-test.sh --cleanup
```

## 📈 预期输出示例

```
🐳 ==================================================
🐳 NautilusTrader Docker 币安延迟测试启动
🐳 ==================================================
✅ NautilusTrader版本: 1.220.0
✅ 已订阅 API3 现货 orderbook
✅ 已订阅 API3 合约 orderbook
✅ 已订阅 OM 现货 orderbook
✅ 已订阅 OM 合约 orderbook
✅ 已订阅 CFX 现货 orderbook
✅ 已订阅 CFX 合约 orderbook

📊 实时延迟统计 - 运行时间: 30.0秒
📈 总消息数: 3,847
🌍 全局延迟: 平均=14.23ms, 中位=12.45ms
==================================================

💰 API3:
  🏪 现货:
     消息: 1,247 | 平均: 15.32ms | 中位: 13.45ms
     范围: 8.45 - 45.67ms
     分布: <10ms(23%) 10-50ms(75%) >50ms(2%)
  📈 合约:
     消息: 1,189 | 平均: 12.78ms | 中位: 11.23ms
     范围: 7.23 - 38.91ms
     分布: <10ms(31%) 10-50ms(68%) >50ms(1%)

💰 OM:
  🏪 现货:
     消息: 1,156 | 平均: 16.45ms | 中位: 14.67ms
     范围: 9.12 - 52.34ms
     分布: <10ms(18%) 10-50ms(79%) >50ms(3%)
  📈 合约:
     消息: 1,203 | 平均: 13.89ms | 中位: 12.34ms
     范围: 8.67 - 41.23ms
     分布: <10ms(25%) 10-50ms(74%) >50ms(1%)

💰 CFX:
  🏪 现货:
     消息: 1,098 | 平均: 17.23ms | 中位: 15.78ms
     范围: 10.34 - 48.56ms
     分布: <10ms(15%) 10-50ms(82%) >50ms(3%)
  📈 合约:
     消息: 1,167 | 平均: 14.56ms | 中位: 13.45ms
     范围: 9.45 - 39.78ms
     分布: <10ms(22%) 10-50ms(77%) >50ms(1%)

📄 详细报告已保存: /app/reports/binance_latency_nautilus_20240115_143022.txt
```

## 🔍 故障排除

### 1. Docker未安装或未启动
```bash
# 错误信息: docker: command not found
# 解决方案: 安装并启动Docker Desktop
```

### 2. 构建失败
```bash
# 可能原因: 网络问题、依赖下载失败
# 解决方案: 检查网络连接，重试构建
docker system prune -f  # 清理缓存
./run-docker-latency-test.sh --build-only
```

### 3. 容器运行失败
```bash
# 查看容器日志
docker logs nautilus-binance-latency-test

# 进入容器调试
docker run --rm -it nautilus-latency-test bash
```

### 4. 权限问题
```bash
# 确保脚本有执行权限
chmod +x run-docker-latency-test.sh
chmod +x docker/docker_entrypoint.sh
```

## 📋 系统要求

- **操作系统**: macOS 10.15+, Linux, Windows 10+ with WSL2
- **内存**: 至少4GB可用内存（构建时）
- **磁盘空间**: 至少2GB可用空间
- **网络**: 稳定的互联网连接（访问币安API）

## 🎯 测试结果解读

- **延迟 < 10ms**: 优秀，适合高频交易
- **延迟 10-50ms**: 良好，适合大多数交易策略
- **延迟 > 50ms**: 较高，可能影响交易性能

## 🤝 支持

如果遇到问题，请：
1. 检查Docker是否正常运行
2. 确认网络连接稳定
3. 查看容器日志获取详细错误信息
4. 参考故障排除部分

---

**注意**: 这个工具仅用于测试网络延迟，不涉及实际交易。无需提供API密钥。
