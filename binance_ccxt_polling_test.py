#!/usr/bin/env python3
"""
币安Orderbook延迟测试工具 - CCXT轮询版本
通过REST API轮询获取orderbook数据，测试延迟
测试API3、OM、CFX三个币种的现货和合约
"""

import asyncio
import time
import statistics
import sys
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List

try:
    import ccxt
except ImportError:
    print("安装CCXT依赖...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt"])
    import ccxt


class BinanceCCXTPollingTester:
    """基于CCXT REST API的币安延迟测试器"""
    
    def __init__(self):
        # 测试配置
        self.test_symbols = ["API3/USDT", "OM/USDT", "CFX/USDT"]
        self.test_duration = 60  # 秒
        self.polling_interval = 0.1  # 100ms轮询间隔
        
        # 延迟数据存储
        self.latencies: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        self.recent_latencies: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=50)))
        self.message_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # 测试状态
        self.running = True
        self.start_time = None
        self.total_messages = 0
        
        # 创建交易所实例
        self.spot_exchange = None
        self.futures_exchange = None

    def create_exchanges(self):
        """创建币安现货和合约交易所实例"""
        try:
            # 现货交易所
            self.spot_exchange = ccxt.binance({
                'sandbox': False,
                'enableRateLimit': False,  # 关闭限流以获得更快响应
                'timeout': 5000,
                'options': {
                    'defaultType': 'spot',
                }
            })
            
            # 合约交易所
            self.futures_exchange = ccxt.binance({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 5000,
                'options': {
                    'defaultType': 'future',
                }
            })
            
            print("✅ CCXT交易所实例创建成功", flush=True)
            return True
            
        except Exception as e:
            print(f"❌ 创建交易所实例失败: {e}", flush=True)
            return False

    async def poll_orderbook(self, exchange, symbol: str, market_type: str):
        """轮询orderbook数据"""
        try:
            print(f"📡 开始轮询 {symbol} {market_type}", flush=True)
            
            while self.running:
                try:
                    # 记录请求开始时间
                    request_start = time.time() * 1000
                    
                    # 获取orderbook数据（5档深度）
                    orderbook = exchange.fetch_order_book(symbol, limit=5)
                    
                    # 记录响应时间
                    response_time = time.time() * 1000
                    
                    # 计算延迟
                    latency_ms = response_time - request_start
                    
                    # 存储延迟数据
                    base_symbol = symbol.split('/')[0]  # API3/USDT -> API3
                    self.latencies[base_symbol][market_type].append(latency_ms)
                    self.recent_latencies[base_symbol][market_type].append(latency_ms)
                    self.message_counts[base_symbol][market_type] += 1
                    self.total_messages += 1
                    
                    # 每收到10条消息显示进度
                    if self.total_messages % 10 == 0:
                        elapsed = time.time() - self.start_time if self.start_time else 0
                        market_name = "现货" if market_type == "spot" else "合约"
                        print(f"📊 #{self.total_messages} {base_symbol} {market_name}: {latency_ms:.2f}ms (运行{elapsed:.0f}s)", flush=True)
                    
                    # 等待下次轮询
                    await asyncio.sleep(self.polling_interval)
                    
                except Exception as e:
                    if "rate limit" not in str(e).lower():
                        print(f"⚠️  {symbol} {market_type} 获取错误: {e}", flush=True)
                    await asyncio.sleep(0.5)  # 出错时等待更长时间
                    
        except Exception as e:
            print(f"❌ {symbol} {market_type} 轮询失败: {e}", flush=True)

    async def print_stats_periodically(self):
        """定期打印统计信息"""
        await asyncio.sleep(3)  # 等待开始收集数据
        
        while self.running:
            await asyncio.sleep(10)  # 每10秒
            if self.start_time:
                self.print_realtime_stats()

    def print_realtime_stats(self):
        """打印实时统计信息"""
        elapsed = time.time() - self.start_time
        
        print(f"\n📊 " + "="*80, flush=True)
        print(f"📊 实时延迟统计报告 - 运行时间: {elapsed:.1f}秒", flush=True)
        print(f"📊 " + "="*80, flush=True)
        print(f"📈 总请求数: {self.total_messages:,}", flush=True)
        print(f"📡 请求频率: {self.total_messages/elapsed:.1f}次/秒", flush=True)
        
        # 计算全局延迟
        all_recent_latencies = []
        for symbol in ["API3", "OM", "CFX"]:
            for market_type in ["spot", "futures"]:
                recent_data = list(self.recent_latencies[symbol][market_type])
                all_recent_latencies.extend(recent_data)
        
        if all_recent_latencies:
            global_avg = statistics.mean(all_recent_latencies)
            global_median = statistics.median(all_recent_latencies)
            print(f"🌍 全局延迟: 平均={global_avg:.3f}ms, 中位={global_median:.3f}ms", flush=True)
            print("-" * 80, flush=True)
        
        # 各币种统计
        for symbol in ["API3", "OM", "CFX"]:
            print(f"\n💰 {symbol} 延迟统计:", flush=True)
            
            for market_type in ["spot", "futures"]:
                recent_data = list(self.recent_latencies[symbol][market_type])
                total_count = self.message_counts[symbol][market_type]
                
                if recent_data:
                    avg_latency = statistics.mean(recent_data)
                    min_latency = min(recent_data)
                    max_latency = max(recent_data)
                    median_latency = statistics.median(recent_data)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    
                    print(f"  {market_emoji} {market_name}: 请求={total_count:,}, 平均={avg_latency:.2f}ms, 中位={median_latency:.2f}ms, 范围={min_latency:.2f}-{max_latency:.2f}ms", flush=True)
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_emoji} {market_name}: ⏳ 等待数据...", flush=True)

    def generate_final_report(self):
        """生成最终详细报告"""
        print(f"\n🏁 " + "="*90, flush=True)
        print("🏁 币安Orderbook延迟测试最终报告 (CCXT轮询版本)", flush=True)
        print("🏁 " + "="*90, flush=True)
        
        if not self.start_time:
            print("❌ 测试未正常启动", flush=True)
            return
            
        test_duration = time.time() - self.start_time
        print(f"🕐 测试开始时间: {datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        print(f"⏱️  实际测试时长: {test_duration:.1f}秒", flush=True)
        print(f"📊 总请求数量: {self.total_messages:,}", flush=True)
        print(f"📈 平均请求频率: {self.total_messages/test_duration:.1f}次/秒", flush=True)
        
        # 生成报告内容
        report_lines = []
        report_lines.append("币安Orderbook延迟测试详细报告 (CCXT轮询版本)")
        report_lines.append("=" * 80)
        report_lines.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"测试时长: {test_duration:.1f}秒")
        report_lines.append(f"总请求数: {self.total_messages:,}")
        report_lines.append(f"轮询间隔: {self.polling_interval*1000:.0f}ms")
        report_lines.append(f"测试币种: API3, OM, CFX")
        report_lines.append(f"市场类型: 现货 + USDT永续合约")
        report_lines.append("")
        
        # 计算全局统计
        all_latencies = []
        for symbol in ["API3", "OM", "CFX"]:
            for market_type in ["spot", "futures"]:
                all_latencies.extend(self.latencies[symbol][market_type])
        
        if all_latencies:
            global_avg = statistics.mean(all_latencies)
            global_median = statistics.median(all_latencies)
            global_std = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0
            global_min = min(all_latencies)
            global_max = max(all_latencies)
            
            print(f"\n🌍 全局延迟统计 (REST API响应时间):", flush=True)
            print(f"   📊 总样本数: {len(all_latencies):,}", flush=True)
            print(f"   ⚡ 平均延迟: {global_avg:.3f}ms", flush=True)
            print(f"   🎯 中位延迟: {global_median:.3f}ms", flush=True)
            print(f"   📈 标准差: {global_std:.3f}ms", flush=True)
            print(f"   📏 延迟范围: {global_min:.3f} - {global_max:.3f}ms", flush=True)
            
            report_lines.append("全局延迟统计:")
            report_lines.append(f"  总样本数: {len(all_latencies):,}")
            report_lines.append(f"  平均延迟: {global_avg:.3f}ms")
            report_lines.append(f"  中位延迟: {global_median:.3f}ms")
            report_lines.append(f"  标准差: {global_std:.3f}ms")
            report_lines.append("")
        
        print("\n" + "-" * 90, flush=True)
        
        # 各币种详细统计
        for symbol in ["API3", "OM", "CFX"]:
            print(f"\n💰 {symbol} 详细延迟分析:", flush=True)
            report_lines.append(f"{symbol} 详细统计:")
            
            for market_type in ["spot", "futures"]:
                all_symbol_latencies = self.latencies[symbol][market_type]
                total_count = self.message_counts[symbol][market_type]
                
                if all_symbol_latencies:
                    # 基础统计
                    avg_latency = statistics.mean(all_symbol_latencies)
                    min_latency = min(all_symbol_latencies)
                    max_latency = max(all_symbol_latencies)
                    median_latency = statistics.median(all_symbol_latencies)
                    std_latency = statistics.stdev(all_symbol_latencies) if len(all_symbol_latencies) > 1 else 0
                    
                    # 延迟分布分析
                    fast_count = sum(1 for l in all_symbol_latencies if l < 50)
                    medium_count = sum(1 for l in all_symbol_latencies if 50 <= l < 200)
                    slow_count = sum(1 for l in all_symbol_latencies if l >= 200)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    symbol_display = f"{symbol}{' 永续' if market_type == 'futures' else ''}"
                    
                    # 详细控制台输出
                    print(f"  {market_emoji} {market_name} ({symbol_display}):", flush=True)
                    print(f"     📊 请求数量: {total_count:,}", flush=True)
                    print(f"     ⚡ 平均延迟: {avg_latency:.3f}ms", flush=True)
                    print(f"     📈 标准差: {std_latency:.3f}ms", flush=True)
                    print(f"     🎯 中位延迟: {median_latency:.3f}ms", flush=True)
                    print(f"     📏 延迟范围: {min_latency:.3f} - {max_latency:.3f}ms", flush=True)
                    print(f"     📈 分布: <50ms({fast_count/total_count*100:.1f}%) 50-200ms({medium_count/total_count*100:.1f}%) >=200ms({slow_count/total_count*100:.1f}%)", flush=True)
                    
                    # 报告文件内容
                    report_lines.append(f"  {market_name}: 请求={total_count:,}, 平均={avg_latency:.2f}ms, 中位={median_latency:.2f}ms")
                    report_lines.append(f"    延迟范围: {min_latency:.3f} - {max_latency:.3f}ms")
                    report_lines.append("")
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_emoji} {market_name}: ❌ 未收到数据", flush=True)
                    report_lines.append(f"  {market_name}: 未收到数据")
        
        # 保存最终报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/binance_ccxt_polling_report_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 详细报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n🎉 测试完成！", flush=True)

    async def run_test(self):
        """运行完整的延迟测试"""
        print("🚀 " + "="*80, flush=True)
        print("🚀 币安Orderbook延迟测试工具 (CCXT轮询版本)", flush=True)
        print("🚀 " + "="*80, flush=True)
        print("🎯 测试币种: API3, OM, CFX", flush=True)
        print("📊 测试市场: 现货 + USDT永续合约", flush=True)
        print(f"⏱️  测试时长: {self.test_duration}秒", flush=True)
        print("📈 Orderbook深度: 5档", flush=True)
        print(f"📡 轮询间隔: {self.polling_interval*1000:.0f}ms", flush=True)
        print("💡 注意: 这是REST API响应时间，不是WebSocket推送延迟", flush=True)
        print("🚀 " + "="*80, flush=True)
        
        # 创建交易所实例
        if not self.create_exchanges():
            print("❌ 无法创建交易所实例，测试终止", flush=True)
            return
        
        print("📡 正在开始REST API轮询...", flush=True)
        self.start_time = time.time()
        
        # 创建所有轮询任务
        tasks = []
        
        # 现货轮询任务
        for symbol in self.test_symbols:
            task = asyncio.create_task(
                self.poll_orderbook(self.spot_exchange, symbol, "spot"),
                name=f"spot_{symbol}"
            )
            tasks.append(task)
        
        # 合约轮询任务
        for symbol in self.test_symbols:
            task = asyncio.create_task(
                self.poll_orderbook(self.futures_exchange, symbol, "futures"),
                name=f"futures_{symbol}"
            )
            tasks.append(task)
        
        # 统计任务
        tasks.append(asyncio.create_task(self.print_stats_periodically(), name="stats"))
        
        # 测试结束定时器
        async def stop_test():
            await asyncio.sleep(self.test_duration)
            self.running = False
            print(f"\n⏰ {self.test_duration}秒测试时间到，正在停止...", flush=True)
        
        tasks.append(asyncio.create_task(stop_test(), name="timer"))
        
        try:
            print("🚀 开始数据收集...", flush=True)
            print("💡 每10次请求显示进度，每10秒显示详细统计", flush=True)
            print("-" * 80, flush=True)
            
            # 等待所有任务完成
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except KeyboardInterrupt:
            print("\n⏹️  收到中断信号，正在停止测试...", flush=True)
            self.running = False
        finally:
            # 生成最终报告
            self.generate_final_report()


async def main():
    """主函数"""
    print("🐳 Docker CCXT 币安延迟测试 (轮询版本)", flush=True)
    print("="*60, flush=True)
    
    # 创建测试器并运行
    tester = BinanceCCXTPollingTester()
    await tester.run_test()


if __name__ == "__main__":
    try:
        print("🔍 检查CCXT依赖...", flush=True)
        import ccxt
        print(f"✅ CCXT版本: {ccxt.__version__}", flush=True)
    except ImportError:
        print("📦 安装CCXT依赖...", flush=True)
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt"])
        print("✅ CCXT安装完成", flush=True)
    
    print("🚀 启动CCXT轮询延迟测试...", flush=True)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  测试被用户中断", flush=True)
    except Exception as e:
        print(f"❌ 测试失败: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        print("👋 测试结束", flush=True)
