#!/usr/bin/env python3
"""
币安现货和合约Orderbook延迟测试工具 - 完整版
测试API3、OM、CFX三个币种的现货和合约orderbook 5档数据延迟
使用NautilusTrader框架，测算1分钟求平均，不同币种和交易所分开计算
"""

import time
import statistics
import sys
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from nautilus_trader.adapters.binance import BINANCE
from nautilus_trader.adapters.binance import BinanceAccountType
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig
from nautilus_trader.common.events import TimeEvent


class BinanceLatencyTestConfig(StrategyConfig, frozen=True):
    """币安延迟测试配置"""
    pass


class BinanceLatencyTestStrategy(Strategy):
    """
    币安延迟测试策略
    
    功能：
    1. 测试API3、OM、CFX三个币种
    2. 分别测试现货和USDT永续合约
    3. 计算orderbook 5档数据的传输延迟
    4. 每10秒输出实时统计，1分钟后生成完整报告
    5. 不同币种和市场类型分开统计
    """
    
    def __init__(self, config: BinanceLatencyTestConfig) -> None:
        super().__init__(config)
        
        # 测试币种配置
        self.test_symbols = ["API3", "OM", "CFX"]
        self.test_duration_minutes = 1
        self.orderbook_depth = 5
        
        # 延迟数据存储 - 按币种和市场类型分组
        self.latencies: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        self.recent_latencies: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=100)))
        self.message_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # 测试状态
        self._start_time = None
        self._total_messages = 0
        self._subscription_count = 0

    def on_start(self) -> None:
        """策略启动 - 订阅数据并设置定时器"""
        print("🚀 " + "="*80, flush=True)
        print("🚀 币安Orderbook延迟测试工具 (NautilusTrader完整版)", flush=True)
        print("🚀 " + "="*80, flush=True)
        
        self._start_time = time.time()
        
        print("📊 测试配置:", flush=True)
        print(f"   🎯 测试币种: {', '.join(self.test_symbols)}", flush=True)
        print(f"   📈 市场类型: 现货 + USDT永续合约", flush=True)
        print(f"   ⏱️  测试时长: {self.test_duration_minutes}分钟", flush=True)
        print(f"   📊 订单簿深度: {self.orderbook_depth}档", flush=True)
        print(f"   📡 统计间隔: 每10秒", flush=True)
        print()
        
        # 订阅所有币种的现货和合约orderbook数据
        print("📡 开始订阅数据流...", flush=True)
        for symbol in self.test_symbols:
            try:
                # 现货订阅
                spot_instrument = InstrumentId.from_str(f"{symbol}USDT.BINANCE")
                self.subscribe_order_book_deltas(
                    instrument_id=spot_instrument,
                    book_type=BookType.L2_MBP,
                    depth=self.orderbook_depth,
                )
                print(f"✅ {symbol} 现货 ({symbol}USDT) 已订阅", flush=True)
                self._subscription_count += 1
                
                # 合约订阅
                futures_instrument = InstrumentId.from_str(f"{symbol}USDT-PERP.BINANCE")
                self.subscribe_order_book_deltas(
                    instrument_id=futures_instrument,
                    book_type=BookType.L2_MBP,
                    depth=self.orderbook_depth,
                )
                print(f"✅ {symbol} 合约 ({symbol}USDT-PERP) 已订阅", flush=True)
                self._subscription_count += 1
                
            except Exception as e:
                print(f"❌ {symbol} 订阅失败: {e}", flush=True)
        
        print(f"\n📡 总共成功订阅 {self._subscription_count}/6 个交易对", flush=True)
        print("🚀 开始数据收集和延迟计算...", flush=True)
        print("-" * 80, flush=True)
        
        # 设置定时器 - 使用正确的API
        try:
            # 实时统计定时器（每10秒）
            self.clock.set_timer(
                name="realtime_stats",
                interval=timedelta(seconds=10),
                callback=self.on_realtime_stats_timer,
            )
            
            # 测试结束定时器（1分钟）
            self.clock.set_timer(
                name="test_end",
                interval=timedelta(minutes=self.test_duration_minutes),
                callback=self.on_test_end_timer,
            )
            
            print("✅ 定时器设置成功", flush=True)
            
        except Exception as e:
            print(f"❌ 定时器设置失败: {e}", flush=True)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        处理orderbook增量数据
        计算从交易所时间戳到本地接收时间的延迟
        """
        try:
            # 高精度延迟计算（纳秒级）
            local_receive_time_ns = time.time_ns()
            exchange_event_time_ns = deltas.ts_event
            latency_ms = (local_receive_time_ns - exchange_event_time_ns) / 1_000_000
            
            # 解析交易对信息
            instrument_str = str(deltas.instrument_id)
            symbol, market_type = self._parse_instrument_info(instrument_str)
            
            if symbol and symbol in self.test_symbols:
                # 按币种和市场类型分别存储延迟数据
                self.latencies[symbol][market_type].append(latency_ms)
                self.recent_latencies[symbol][market_type].append(latency_ms)
                self.message_counts[symbol][market_type] += 1
                self._total_messages += 1
                
                # 每收到25条消息显示一次实时进度
                if self._total_messages % 25 == 0:
                    elapsed = time.time() - self._start_time
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"📊 #{self._total_messages} {symbol} {market_name}: {latency_ms:.2f}ms (运行{elapsed:.0f}s)", flush=True)
                
        except Exception as e:
            print(f"⚠️  处理orderbook数据时出错: {e}", flush=True)

    def _parse_instrument_info(self, instrument_str: str) -> tuple[Optional[str], str]:
        """
        解析交易对字符串，提取币种和市场类型
        
        Args:
            instrument_str: 如 "API3USDT.BINANCE" 或 "API3USDT-PERP.BINANCE"
            
        Returns:
            (币种, 市场类型) 如 ("API3", "spot") 或 ("API3", "futures")
        """
        try:
            # 移除交易所后缀
            base_part = instrument_str.split('.')[0]
            
            if '-PERP' in base_part:
                # USDT永续合约
                symbol = base_part.replace('-PERP', '').replace('USDT', '')
                return symbol, "futures"
            else:
                # 现货
                symbol = base_part.replace('USDT', '')
                return symbol, "spot"
                
        except Exception as e:
            print(f"⚠️  解析交易对失败 {instrument_str}: {e}", flush=True)
            return None, "unknown"

    def on_realtime_stats_timer(self, event: TimeEvent) -> None:
        """实时统计定时器回调 - 每10秒执行"""
        try:
            if not self._start_time:
                return
                
            elapsed = time.time() - self._start_time
            
            print(f"\n📊 " + "="*70, flush=True)
            print(f"📊 实时延迟统计报告 - 运行时间: {elapsed:.1f}秒", flush=True)
            print(f"📊 " + "="*70, flush=True)
            print(f"📈 总消息数: {self._total_messages:,}", flush=True)
            
            # 计算全局延迟统计
            all_recent_latencies = []
            for symbol in self.test_symbols:
                for market_type in ["spot", "futures"]:
                    recent_data = list(self.recent_latencies[symbol][market_type])
                    all_recent_latencies.extend(recent_data)
            
            if all_recent_latencies:
                global_avg = statistics.mean(all_recent_latencies)
                global_median = statistics.median(all_recent_latencies)
                print(f"🌍 全局延迟: 平均={global_avg:.3f}ms, 中位={global_median:.3f}ms", flush=True)
                print("-" * 70, flush=True)
            
            # 各币种详细统计
            for symbol in self.test_symbols:
                print(f"\n💰 {symbol} 延迟统计:", flush=True)
                
                for market_type in ["spot", "futures"]:
                    recent_data = list(self.recent_latencies[symbol][market_type])
                    total_count = self.message_counts[symbol][market_type]
                    
                    if recent_data:
                        avg_latency = statistics.mean(recent_data)
                        min_latency = min(recent_data)
                        max_latency = max(recent_data)
                        median_latency = statistics.median(recent_data)
                        
                        # 延迟分布统计
                        fast_count = sum(1 for l in recent_data if l < 10)
                        medium_count = sum(1 for l in recent_data if 10 <= l < 50)
                        slow_count = sum(1 for l in recent_data if l >= 50)
                        total_recent = len(recent_data)
                        
                        market_emoji = "🏪" if market_type == "spot" else "📈"
                        market_name = "现货" if market_type == "spot" else "合约"
                        symbol_display = f"{symbol}USDT{'-PERP' if market_type == 'futures' else ''}"
                        
                        print(f"  {market_emoji} {market_name} ({symbol_display}):", flush=True)
                        print(f"     📊 消息数量: {total_count:,}", flush=True)
                        print(f"     ⚡ 平均延迟: {avg_latency:.3f}ms", flush=True)
                        print(f"     🎯 中位延迟: {median_latency:.3f}ms", flush=True)
                        print(f"     📏 延迟范围: {min_latency:.3f} - {max_latency:.3f}ms", flush=True)
                        print(f"     📈 分布: <10ms({fast_count/total_recent*100:.1f}%) 10-50ms({medium_count/total_recent*100:.1f}%) >=50ms({slow_count/total_recent*100:.1f}%)", flush=True)
                    else:
                        market_emoji = "🏪" if market_type == "spot" else "📈"
                        market_name = "现货" if market_type == "spot" else "合约"
                        print(f"  {market_emoji} {market_name}: ⏳ 等待数据...", flush=True)
                        
        except Exception as e:
            print(f"❌ 实时统计错误: {e}", flush=True)

    def on_test_end_timer(self, event: TimeEvent) -> None:
        """测试结束定时器回调 - 1分钟后执行"""
        try:
            print("\n🏁 " + "="*80, flush=True)
            print("🏁 测试时间到！生成最终延迟测试报告...", flush=True)
            print("🏁 " + "="*80, flush=True)
            
            self._generate_comprehensive_report()
            self.stop()
            
        except Exception as e:
            print(f"❌ 结束测试错误: {e}", flush=True)

    def _generate_comprehensive_report(self) -> None:
        """生成综合的延迟测试报告"""
        if not self._start_time:
            print("❌ 测试未正常启动", flush=True)
            return
            
        test_duration = time.time() - self._start_time
        
        print(f"\n📋 币安Orderbook延迟测试最终报告", flush=True)
        print("=" * 100, flush=True)
        print(f"🕐 测试开始时间: {datetime.fromtimestamp(self._start_time).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        print(f"⏱️  实际测试时长: {test_duration:.1f}秒", flush=True)
        print(f"📊 总消息数量: {self._total_messages:,}", flush=True)
        print(f"📡 成功订阅数: {self._subscription_count}/6", flush=True)
        print(f"📈 平均消息频率: {self._total_messages/test_duration:.1f}条/秒", flush=True)
        
        # 生成详细报告内容
        report_lines = []
        report_lines.append("币安Orderbook延迟测试详细报告")
        report_lines.append("=" * 80)
        report_lines.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"测试时长: {test_duration:.1f}秒")
        report_lines.append(f"总消息数: {self._total_messages:,}")
        report_lines.append(f"测试币种: {', '.join(self.test_symbols)}")
        report_lines.append(f"订单簿深度: {self.orderbook_depth}档")
        report_lines.append("")
        
        # 计算全局延迟统计
        all_latencies = []
        for symbol in self.test_symbols:
            for market_type in ["spot", "futures"]:
                all_latencies.extend(self.latencies[symbol][market_type])
        
        if all_latencies:
            global_avg = statistics.mean(all_latencies)
            global_median = statistics.median(all_latencies)
            global_std = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0
            global_min = min(all_latencies)
            global_max = max(all_latencies)
            
            # 全局分位数
            sorted_latencies = sorted(all_latencies)
            p95_idx = int(len(sorted_latencies) * 0.95)
            p99_idx = int(len(sorted_latencies) * 0.99)
            p95_latency = sorted_latencies[p95_idx] if len(sorted_latencies) > 20 else global_max
            p99_latency = sorted_latencies[p99_idx] if len(sorted_latencies) > 100 else global_max
            
            print(f"\n🌍 全局延迟统计 (所有币种所有市场):", flush=True)
            print(f"   📊 总样本数: {len(all_latencies):,}", flush=True)
            print(f"   ⚡ 平均延迟: {global_avg:.3f}ms", flush=True)
            print(f"   🎯 中位延迟: {global_median:.3f}ms", flush=True)
            print(f"   📈 标准差: {global_std:.3f}ms", flush=True)
            print(f"   📏 延迟范围: {global_min:.3f} - {global_max:.3f}ms", flush=True)
            print(f"   📊 P95延迟: {p95_latency:.3f}ms", flush=True)
            print(f"   📊 P99延迟: {p99_latency:.3f}ms", flush=True)
            
            report_lines.append("全局延迟统计:")
            report_lines.append(f"  总样本数: {len(all_latencies):,}")
            report_lines.append(f"  平均延迟: {global_avg:.3f}ms")
            report_lines.append(f"  中位延迟: {global_median:.3f}ms")
            report_lines.append(f"  标准差: {global_std:.3f}ms")
            report_lines.append(f"  P95延迟: {p95_latency:.3f}ms")
            report_lines.append(f"  P99延迟: {p99_latency:.3f}ms")
            report_lines.append("")
        
        print("\n" + "-" * 100, flush=True)
        
        # 各币种分别统计
        for symbol in self.test_symbols:
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
                    
                    # 分位数统计
                    if len(all_symbol_latencies) >= 20:
                        sorted_symbol_latencies = sorted(all_symbol_latencies)
                        p90_idx = int(len(sorted_symbol_latencies) * 0.90)
                        p95_idx = int(len(sorted_symbol_latencies) * 0.95)
                        p99_idx = int(len(sorted_symbol_latencies) * 0.99)
                        p90_latency = sorted_symbol_latencies[p90_idx]
                        p95_latency = sorted_symbol_latencies[p95_idx]
                        p99_latency = sorted_symbol_latencies[p99_idx]
                    else:
                        p90_latency = p95_latency = p99_latency = max_latency
                    
                    # 延迟分布分析
                    excellent_count = sum(1 for l in all_symbol_latencies if l < 5)
                    good_count = sum(1 for l in all_symbol_latencies if 5 <= l < 15)
                    fair_count = sum(1 for l in all_symbol_latencies if 15 <= l < 50)
                    poor_count = sum(1 for l in all_symbol_latencies if l >= 50)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    symbol_display = f"{symbol}USDT{'-PERP' if market_type == 'futures' else ''}"
                    
                    # 详细控制台输出
                    print(f"  {market_emoji} {market_name} ({symbol_display}):", flush=True)
                    print(f"     📊 消息数量: {total_count:,}", flush=True)
                    print(f"     ⚡ 平均延迟: {avg_latency:.3f}ms", flush=True)
                    print(f"     📈 标准差: {std_latency:.3f}ms", flush=True)
                    print(f"     🎯 中位延迟: {median_latency:.3f}ms", flush=True)
                    print(f"     📊 分位延迟: P90={p90_latency:.3f}ms P95={p95_latency:.3f}ms P99={p99_latency:.3f}ms", flush=True)
                    print(f"     📏 延迟范围: {min_latency:.3f} - {max_latency:.3f}ms", flush=True)
                    print(f"     🚀 延迟分布:", flush=True)
                    print(f"         优秀(<5ms): {excellent_count:,}条 ({excellent_count/total_count*100:.1f}%)", flush=True)
                    print(f"         良好(5-15ms): {good_count:,}条 ({good_count/total_count*100:.1f}%)", flush=True)
                    print(f"         一般(15-50ms): {fair_count:,}条 ({fair_count/total_count*100:.1f}%)", flush=True)
                    print(f"         较差(>=50ms): {poor_count:,}条 ({poor_count/total_count*100:.1f}%)", flush=True)
                    
                    # 报告文件内容
                    report_lines.append(f"  {market_name} ({symbol_display}):")
                    report_lines.append(f"    消息数量: {total_count:,}")
                    report_lines.append(f"    平均延迟: {avg_latency:.3f}ms")
                    report_lines.append(f"    中位延迟: {median_latency:.3f}ms")
                    report_lines.append(f"    P95延迟: {p95_latency:.3f}ms")
                    report_lines.append(f"    延迟范围: {min_latency:.3f} - {max_latency:.3f}ms")
                    report_lines.append(f"    分布: 优秀({excellent_count/total_count*100:.1f}%) 良好({good_count/total_count*100:.1f}%) 一般({fair_count/total_count*100:.1f}%) 较差({poor_count/total_count*100:.1f}%)")
                    report_lines.append("")
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_emoji} {market_name}: ❌ 未收到数据", flush=True)
                    report_lines.append(f"  {market_name}: 未收到数据")
        
        # 保存详细报告文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_complete_latency_report_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 详细报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n" + "🏁 " + "="*100, flush=True)
        print("🏁 币安Orderbook延迟测试完成！感谢使用NautilusTrader", flush=True)
        print("🏁 " + "="*100, flush=True)

    def on_stop(self) -> None:
        """策略停止时的清理工作"""
        print("🛑 策略正在停止...", flush=True)


def main():
    """
    主函数 - 币安延迟测试
    
    要求：
    1. 设置BINANCE_API_KEY和BINANCE_API_SECRET环境变量
    2. 确保网络连接稳定
    3. 测试将运行1分钟
    """
    print("🐳 " + "="*90, flush=True)
    print("🐳 NautilusTrader 币安Orderbook延迟测试工具 - 完整版", flush=True)
    print("🐳 " + "="*90, flush=True)
    print("🎯 测试目标: API3, OM, CFX 现货和合约延迟分析", flush=True)
    print("⏱️  测试时长: 1分钟", flush=True)
    print("📊 数据深度: 5档orderbook", flush=True)
    print("📡 统计间隔: 每10秒", flush=True)
    print("🐳 " + "="*90, flush=True)
    
    # 验证API凭证
    api_key = os.environ.get('BINANCE_API_KEY', '')
    api_secret = os.environ.get('BINANCE_API_SECRET', '')
    
    if not api_key or not api_secret:
        print("❌ 缺少币安API凭证", flush=True)
        print("💡 请设置环境变量:", flush=True)
        print("   export BINANCE_API_KEY=your_api_key", flush=True)
        print("   export BINANCE_API_SECRET=your_api_secret", flush=True)
        return
    
    print(f"✅ API密钥验证: {api_key[:10]}...{api_key[-4:]}", flush=True)
    
    try:
        # 创建优化的交易节点配置
        config = TradingNodeConfig(
            trader_id=TraderId("BINANCE-LATENCY-COMPLETE-TEST"),
            logging=LoggingConfig(
                log_level="WARN",  # 减少日志噪音
                use_pyo3=True,
                log_colors=False,  # Docker环境下关闭颜色
            ),
            data_clients={
                BINANCE: BinanceDataClientConfig(
                    api_key=api_key,
                    api_secret=api_secret,
                    account_type=BinanceAccountType.SPOT,  # 使用SPOT配置获取所有公开数据
                    testnet=False,  # 使用生产环境
                    instrument_provider=InstrumentProviderConfig(load_all=False),  # 不自动加载所有仪器
                ),
            },
            timeout_connection=20.0,
            timeout_disconnection=5.0,
        )
        
        print("🏗️  创建交易节点...", flush=True)
        node = TradingNode(config=config)
        
        print("⚙️  配置延迟测试策略...", flush=True)
        strategy = BinanceLatencyTestStrategy(config=BinanceLatencyTestConfig())
        
        # 注册策略和客户端工厂
        node.trader.add_strategy(strategy)
        node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
        
        print("🔧 构建交易节点...", flush=True)
        node.build()
        
        print("🚀 启动延迟测试...", flush=True)
        print("💡 提示: 按 Ctrl+C 可随时停止测试", flush=True)
        print("📊 进度: 每25条消息显示进度，每10秒显示详细统计", flush=True)
        print()
        
        # 运行测试
        node.run()
        
    except KeyboardInterrupt:
        print("\n⏹️  收到中断信号，正在优雅停止测试...", flush=True)
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        # 清理资源
        print("🧹 正在清理资源...", flush=True)
        try:
            if 'node' in locals():
                node.dispose()
        except Exception as e:
            print(f"⚠️  清理资源时出错: {e}", flush=True)
        
        print("👋 测试结束，感谢使用！", flush=True)


if __name__ == "__main__":
    main()
