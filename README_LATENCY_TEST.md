# 🚀 NautilusTrader Docker 币安延迟测试

这是一个基于NautilusTrader框架的币安orderbook延迟测试工具，使用Docker容器化部署，支持API3、OM、CFX三个币种的现货和合约延迟测试。

## 📦 项目文件

```
nautilus_trader/
├── 🐳 Docker相关文件
│   ├── Dockerfile.latency-test          # 延迟测试专用Dockerfile
│   ├── docker-compose.latency.yml       # Docker Compose配置
│   ├── run-docker-latency-test.sh       # 主运行脚本
│   ├── quick-test.sh                    # 快速测试脚本
│   └── docker/
│       ├── latency_test_nautilus.py     # 核心测试脚本
│       └── docker_entrypoint.sh         # 容器入口脚本
├── 📚 文档
│   ├── README_LATENCY_TEST.md           # 本文件
│   └── DOCKER_SETUP_GUIDE.md           # 详细安装指南
└── 📊 输出目录
    └── reports/                         # 测试报告输出目录
```

## 🎯 测试功能

- ✅ **多币种支持**: API3, OM, CFX
- ✅ **双市场测试**: 现货 + USDT永续合约
- ✅ **实时统计**: 每10秒输出延迟统计
- ✅ **详细分析**: 平均值、中位数、分位数、分布统计
- ✅ **结果保存**: 自动生成详细报告文件
- ✅ **容器化部署**: 使用Docker避免环境问题

## 🚀 快速开始

### 第一步：安装Docker

**macOS用户**:
```bash
# 使用Homebrew安装（推荐）
brew install --cask docker

# 或者从官网下载安装包
# https://www.docker.com/products/docker-desktop/
```

**启动Docker Desktop并等待完全启动**

### 第二步：运行测试

```bash
# 方法1: 快速测试（推荐新手）
./quick-test.sh

# 方法2: 直接运行完整脚本
./run-docker-latency-test.sh

# 方法3: 使用docker-compose
docker-compose -f docker-compose.latency.yml up --build
```

## 📊 测试输出示例

```
🐳 ==================================================
🐳 NautilusTrader Docker 币安延迟测试启动
🐳 ==================================================
✅ NautilusTrader版本: 1.220.0

📊 实时延迟统计 - 运行时间: 30.0秒
📈 总消息数: 3,847
🌍 全局延迟: 平均=14.23ms, 中位=12.45ms

💰 API3:
  🏪 现货: 消息=1,247 | 平均=15.32ms | 中位=13.45ms | 范围=8.45-45.67ms
  📈 合约: 消息=1,189 | 平均=12.78ms | 中位=11.23ms | 范围=7.23-38.91ms

💰 OM:
  🏪 现货: 消息=1,156 | 平均=16.45ms | 中位=14.67ms | 范围=9.12-52.34ms
  📈 合约: 消息=1,203 | 平均=13.89ms | 中位=12.34ms | 范围=8.67-41.23ms

💰 CFX:
  🏪 现货: 消息=1,098 | 平均=17.23ms | 中位=15.78ms | 范围=10.34-48.56ms
  📈 合约: 消息=1,167 | 平均=14.56ms | 中位=13.45ms | 范围=9.45-39.78ms

📄 详细报告已保存: ./reports/binance_latency_nautilus_20240115_143022.txt
```

## ⚙️ 高级配置

### 自定义测试参数

编辑 `docker/latency_test_nautilus.py`:

```python
strategy_config = DockerBinanceLatencyConfig(
    test_symbols=["API3", "OM", "CFX"],      # 修改币种
    test_duration_minutes=1,                  # 修改测试时长（分钟）
    orderbook_depth=5,                       # 修改深度
    stats_interval_seconds=10,               # 修改统计间隔
)
```

### 脚本选项

```bash
# 显示帮助
./run-docker-latency-test.sh --help

# 仅构建镜像
./run-docker-latency-test.sh --build-only

# 使用docker-compose
./run-docker-latency-test.sh --compose

# 清理Docker资源
./run-docker-latency-test.sh --cleanup
```

## 📈 延迟解读

| 延迟范围 | 评价 | 适用场景 |
|---------|------|----------|
| < 10ms  | 优秀 | 高频交易、套利 |
| 10-50ms | 良好 | 一般交易策略 |
| > 50ms  | 较高 | 可能影响交易性能 |

## 🔧 故障排除

### 常见问题

1. **Docker未安装**
   ```
   解决方案: 安装Docker Desktop并启动
   ```

2. **构建失败**
   ```bash
   # 清理缓存重试
   docker system prune -f
   ./run-docker-latency-test.sh --build-only
   ```

3. **网络连接问题**
   ```
   确保网络稳定，能够访问币安API
   ```

4. **权限问题**
   ```bash
   chmod +x *.sh
   chmod +x docker/*.sh
   ```

### 调试命令

```bash
# 查看容器日志
docker logs nautilus-binance-latency-test

# 进入容器调试
docker run --rm -it nautilus-latency-test bash

# 检查镜像
docker images | grep nautilus
```

## 🎯 技术特点

- **高精度**: 纳秒级时间戳计算
- **实时监控**: 10秒间隔实时统计
- **全面分析**: 包含平均值、中位数、分位数、分布等
- **容器化**: Docker确保环境一致性
- **自动化**: 一键构建、运行、报告
- **可扩展**: 易于添加新币种或修改参数

## 📝 系统要求

- **操作系统**: macOS 10.15+, Linux, Windows 10+ with WSL2
- **内存**: 4GB+ 可用内存（构建时）
- **磁盘**: 2GB+ 可用空间
- **网络**: 稳定的互联网连接

## 🔒 安全说明

- ✅ **无需API密钥**: 仅获取公开市场数据
- ✅ **无交易功能**: 纯延迟测试，不涉及交易
- ✅ **开源透明**: 所有代码可见可审计

## 📞 获取帮助

如果遇到问题：

1. 📖 查看 `DOCKER_SETUP_GUIDE.md` 详细指南
2. 🔍 检查Docker是否正常运行
3. 📊 确认网络连接稳定
4. 📋 查看容器日志获取错误信息

---

**开始测试**: `./quick-test.sh`

**注意**: 首次运行需要构建Docker镜像，可能需要10-20分钟，请耐心等待。
