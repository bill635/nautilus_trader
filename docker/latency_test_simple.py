#!/usr/bin/env python3
"""
币安Orderbook延迟测试脚本 - Docker简化版（适配预编译NautilusTrader）
测试API3, OM, CFX三个币种的现货和合约orderbook 5档数据延迟
"""

import time
import statistics
import signal
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# NautilusTrader imports - 适配预编译版本
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


from dataclasses import field

class SimpleBinanceLatencyConfig(StrategyConfig, frozen=True):
    """简化的币安延迟测试配置"""
    test_symbols: List[str] = field(default_factory=lambda: ["API3", "OM", "CFX"])
    test_duration_minutes: int = 1
    orderbook_depth: int = 5
    stats_interval_seconds: int = 10


class SimpleBinanceLatencyStrategy(Strategy):
    """
    简化的币安延迟测试策略
    专门适配预编译版本的NautilusTrader
    """
    
    def __init__(self, config: SimpleBinanceLatencyConfig) -> None:
        super().__init__(config)
        # 使用私有属性存储配置，避免覆盖父类的config
        self._test_config = config
        
        # 延迟数据存储
        self.latencies: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        self.recent_latencies: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=100)))
        self.message_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # 测试状态 - 使用私有属性避免与父类冲突
        self._test_start_time: Optional[datetime] = None
        self._is_running = True
        self._total_messages = 0
        self._subscription_count = 0

    def on_start(self) -> None:
        """策略启动"""
        print("🐳 " + "="*70)
        print("🐳 Docker + NautilusTrader 币安延迟测试启动")
        print("🐳 " + "="*70)
        
        self._test_start_time = datetime.utcnow()
        
        print(f"📊 测试配置:")
        print(f"   币种: {', '.join(self._test_config.test_symbols)}")
        print(f"   测试时长: {self._test_config.test_duration_minutes}分钟")
        print(f"   订单簿深度: {self._test_config.orderbook_depth}档")
        print()
        
        # 订阅所有币种的现货和合约数据
        for symbol in self._test_config.test_symbols:
            try:
                # 现货订阅
                spot_instrument = InstrumentId.from_str(f"{symbol}USDT.BINANCE")
                self.subscribe_order_book_deltas(
                    instrument_id=spot_instrument,
                    book_type=BookType.L2_MBP,
                    depth=self._test_config.orderbook_depth,
                )
                print(f"✅ 已订阅 {symbol} 现货")
                self._subscription_count += 1
                
                # 合约订阅
                futures_instrument = InstrumentId.from_str(f"{symbol}USDT-PERP.BINANCE")
                self.subscribe_order_book_deltas(
                    instrument_id=futures_instrument,
                    book_type=BookType.L2_MBP,
                    depth=self._test_config.orderbook_depth,
                )
                print(f"✅ 已订阅 {symbol} 合约")
                self._subscription_count += 1
                
            except Exception as e:
                print(f"❌ 订阅 {symbol} 失败: {e}")
        
        print(f"\n📡 成功订阅 {self._subscription_count} 个交易对")
        print("🚀 开始数据收集...")
        print("-" * 70)
        
        # 设置定时器
        self.clock.set_timer(
            name="stats_timer",
            interval=timedelta(seconds=self._test_config.stats_interval_seconds),
            callback=self._print_statistics,
        )
        
        self.clock.set_timer(
            name="end_timer",
            interval=timedelta(minutes=self._test_config.test_duration_minutes),
            callback=self._end_test,
        )

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """处理orderbook增量数据"""
        if not self._is_running:
            return
            
        try:
            # 计算延迟（纳秒精度）
            current_time_ns = time.time_ns()
            exchange_time_ns = deltas.ts_event
            latency_ms = (current_time_ns - exchange_time_ns) / 1_000_000
            
            # 解析交易对信息
            instrument_str = str(deltas.instrument_id)
            symbol, market_type = self._parse_instrument(instrument_str)
            
            if symbol and symbol in self._test_config.test_symbols:
                # 存储延迟数据
                self.latencies[symbol][market_type].append(latency_ms)
                self.recent_latencies[symbol][market_type].append(latency_ms)
                self.message_counts[symbol][market_type] += 1
                self._total_messages += 1
                
        except Exception as e:
            print(f"⚠️  处理orderbook数据错误: {e}")

    def _parse_instrument(self, instrument_str: str) -> tuple[Optional[str], str]:
        """解析交易对字符串"""
        try:
            # 格式: API3USDT.BINANCE 或 API3USDT-PERP.BINANCE
            base_part = instrument_str.split('.')[0]
            
            if '-PERP' in base_part:
                # 合约
                symbol = base_part.replace('-PERP', '').replace('USDT', '')
                return symbol, "futures"
            else:
                # 现货
                symbol = base_part.replace('USDT', '')
                return symbol, "spot"
                
        except Exception as e:
            print(f"⚠️  解析交易对失败 {instrument_str}: {e}")
            return None, "unknown"

    def _print_statistics(self) -> None:
        """打印实时统计信息"""
        if not self._test_start_time or not self._is_running:
            return
            
        elapsed = datetime.utcnow() - self._test_start_time
        
        print(f"\n📊 实时延迟统计 - 运行时间: {elapsed.total_seconds():.1f}秒")
        print(f"📈 总消息数: {self._total_messages:,}")
        print("=" * 70)
        
        # 计算全局统计
        all_recent_latencies = []
        for symbol in self._test_config.test_symbols:
            for market_type in ["spot", "futures"]:
                recent_data = list(self.recent_latencies[symbol][market_type])
                all_recent_latencies.extend(recent_data)
        
        if all_recent_latencies:
            global_avg = statistics.mean(all_recent_latencies)
            global_median = statistics.median(all_recent_latencies)
            print(f"🌍 全局延迟: 平均={global_avg:.2f}ms, 中位={global_median:.2f}ms")
            print("-" * 70)
        
        # 各币种统计
        for symbol in self._test_config.test_symbols:
            print(f"\n💰 {symbol}:")
            
            for market_type in ["spot", "futures"]:
                recent_data = list(self.recent_latencies[symbol][market_type])
                total_count = self.message_counts[symbol][market_type]
                
                if recent_data:
                    avg_latency = statistics.mean(recent_data)
                    min_latency = min(recent_data)
                    max_latency = max(recent_data)
                    median_latency = statistics.median(recent_data)
                    
                    # 延迟分布
                    fast_count = sum(1 for l in recent_data if l < 10)
                    medium_count = sum(1 for l in recent_data if 10 <= l < 50)
                    slow_count = sum(1 for l in recent_data if l >= 50)
                    total_recent = len(recent_data)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    
                    print(f"  {market_emoji} {market_name}: 消息={total_count:,} | 平均={avg_latency:.2f}ms | 中位={median_latency:.2f}ms | 范围={min_latency:.2f}-{max_latency:.2f}ms")
                    print(f"     分布: <10ms({fast_count/total_recent*100:.0f}%) 10-50ms({medium_count/total_recent*100:.0f}%) >50ms({slow_count/total_recent*100:.0f}%)")
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_emoji} {market_name}: ⏳ 等待数据...")

    def _end_test(self) -> None:
        """结束测试并生成最终报告"""
        self._is_running = False
        
        print("\n" + "🏁 " + "="*70)
        print("🏁 测试完成！生成最终报告...")
        print("🏁 " + "="*70)
        
        self._generate_final_report()
        
        # 停止策略
        self.stop()

    def _generate_final_report(self) -> None:
        """生成详细的最终报告"""
        if not self._test_start_time:
            return
            
        test_duration = datetime.utcnow() - self._test_start_time
        
        print(f"\n📋 最终测试报告")
        print("=" * 80)
        print(f"🕐 测试时间: {self._test_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"⏱️  测试时长: {test_duration.total_seconds():.1f}秒")
        print(f"📊 总消息数: {self._total_messages:,}")
        print(f"📡 订阅数量: {self._subscription_count}")
        
        # 生成报告内容
        report_lines = []
        report_lines.append("币安Orderbook延迟测试最终报告 (Docker + NautilusTrader 简化版)")
        report_lines.append("=" * 70)
        report_lines.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"测试时长: {test_duration.total_seconds():.1f}秒")
        report_lines.append(f"总消息数: {self._total_messages:,}")
        report_lines.append(f"测试币种: {', '.join(self._test_config.test_symbols)}")
        report_lines.append("")
        
        # 计算全局统计
        all_latencies = []
        for symbol in self._test_config.test_symbols:
            for market_type in ["spot", "futures"]:
                all_latencies.extend(self.latencies[symbol][market_type])
        
        if all_latencies:
            global_avg = statistics.mean(all_latencies)
            global_median = statistics.median(all_latencies)
            global_std = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0
            
            print(f"\n🌍 全局延迟统计:")
            print(f"   平均延迟: {global_avg:.3f}ms")
            print(f"   中位延迟: {global_median:.3f}ms")
            print(f"   标准差: {global_std:.3f}ms")
            print(f"   总样本数: {len(all_latencies):,}")
            
            report_lines.append("全局统计:")
            report_lines.append(f"  平均延迟: {global_avg:.3f}ms")
            report_lines.append(f"  中位延迟: {global_median:.3f}ms")
            report_lines.append("")
        
        print("\n" + "-" * 80)
        
        # 各币种详细统计
        for symbol in self._test_config.test_symbols:
            print(f"\n💰 {symbol} 详细统计:")
            report_lines.append(f"{symbol} 统计:")
            
            for market_type in ["spot", "futures"]:
                all_symbol_latencies = self.latencies[symbol][market_type]
                total_count = self.message_counts[symbol][market_type]
                
                if all_symbol_latencies:
                    # 基础统计
                    avg_latency = statistics.mean(all_symbol_latencies)
                    min_latency = min(all_symbol_latencies)
                    max_latency = max(all_symbol_latencies)
                    median_latency = statistics.median(all_symbol_latencies)
                    
                    # 延迟分布
                    fast_count = sum(1 for l in all_symbol_latencies if l < 10)
                    medium_count = sum(1 for l in all_symbol_latencies if 10 <= l < 50)
                    slow_count = sum(1 for l in all_symbol_latencies if l >= 50)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    symbol_display = f"{symbol}USDT{'-PERP' if market_type == 'futures' else ''}"
                    
                    # 控制台输出
                    print(f"  {market_emoji} {market_name} ({symbol_display}):")
                    print(f"     📊 消息数量: {total_count:,}")
                    print(f"     ⚡ 平均延迟: {avg_latency:.3f}ms")
                    print(f"     🎯 中位延迟: {median_latency:.3f}ms")
                    print(f"     📏 延迟范围: {min_latency:.3f} - {max_latency:.3f}ms")
                    print(f"     📈 分布: <10ms({fast_count/total_count*100:.1f}%) 10-50ms({medium_count/total_count*100:.1f}%) >=50ms({slow_count/total_count*100:.1f}%)")
                    
                    # 报告文件内容
                    report_lines.append(f"  {market_name}: 消息={total_count:,}, 平均={avg_latency:.2f}ms, 中位={median_latency:.2f}ms")
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_emoji} {market_name}: ❌ 未收到数据")
                    report_lines.append(f"  {market_name}: 未收到数据")
            
            report_lines.append("")
        
        # 保存报告文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_latency_simple_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 详细报告已保存: {filename}")
        except Exception as e:
            print(f"❌ 保存报告失败: {e}")
        
        print("\n" + "🏁 " + "="*80)
        print("🏁 测试完成！感谢使用Docker + NautilusTrader延迟测试工具")
        print("🏁 " + "="*80)

    def on_stop(self) -> None:
        """策略停止时的清理"""
        self._is_running = False
        print("🛑 策略已停止")


def signal_handler(signum, frame):
    """处理中断信号"""
    print(f"\n⚠️  收到信号 {signum}，正在优雅关闭...")
    sys.exit(0)


def main():
    """主函数"""
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🐳 " + "="*80)
    print("🐳 NautilusTrader Docker 币安Orderbook延迟测试工具 (简化版)")
    print("🐳 " + "="*80)
    print("🎯 测试目标: API3, OM, CFX 现货和合约延迟")
    print("⏱️  测试时长: 1分钟")
    print("📊 数据深度: 5档orderbook")
    print("🐳 " + "="*80)
    
    try:
        # 验证NautilusTrader安装
        print("🔍 验证NautilusTrader安装...")
        import nautilus_trader
        print(f"✅ NautilusTrader版本: {nautilus_trader.__version__}")
        
        # 创建交易节点配置
        config = TradingNodeConfig(
            trader_id=TraderId("DOCKER-BINANCE-LATENCY-SIMPLE"),
            logging=LoggingConfig(
                log_level="INFO",
                use_pyo3=True,
                log_colors=False,  # Docker环境下关闭颜色
            ),
            data_clients={
                BINANCE: BinanceDataClientConfig(
                    # 公开市场数据不需要API密钥
                    api_key=None,
                    api_secret=None,
                    account_type=BinanceAccountType.SPOT,
                    testnet=False,
                    instrument_provider=InstrumentProviderConfig(load_all=True),
                ),
            },
            timeout_connection=30.0,
            timeout_disconnection=10.0,
        )
        
        # 创建交易节点
        print("🏗️  创建交易节点...")
        node = TradingNode(config=config)
        
        # 创建策略
        print("⚙️  配置延迟测试策略...")
        strategy_config = SimpleBinanceLatencyConfig(
            test_symbols=["API3", "OM", "CFX"],
            test_duration_minutes=1,
            orderbook_depth=5,
            stats_interval_seconds=10,
        )
        strategy = SimpleBinanceLatencyStrategy(config=strategy_config)
        
        # 添加策略到节点
        node.trader.add_strategy(strategy)
        
        # 注册数据客户端工厂
        node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
        
        # 构建节点
        print("🔧 构建交易节点...")
        node.build()
        
        print("🚀 启动延迟测试...")
        print("💡 提示: 按 Ctrl+C 可随时停止测试")
        print()
        
        # 运行测试
        node.run()
        
    except KeyboardInterrupt:
        print("\n⏹️  收到中断信号，正在停止测试...")
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理资源
        print("🧹 正在清理资源...")
        try:
            if 'node' in locals():
                node.dispose()
        except Exception as e:
            print(f"⚠️  清理资源时出错: {e}")
        
        print("👋 测试结束，再见！")


if __name__ == "__main__":
    main()
