#!/usr/bin/env python3
"""
币安Orderbook延迟测试脚本 - 实时输出版本
解决Docker环境下输出缓冲的问题
"""

import time
import statistics
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import field

# 强制刷新输出
import functools
def flush_print(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()

# NautilusTrader imports
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


class RealtimeLatencyConfig(StrategyConfig, frozen=True):
    """实时延迟测试配置"""
    test_symbols: List[str] = field(default_factory=lambda: ["API3", "OM", "CFX"])
    test_duration_minutes: int = 1
    orderbook_depth: int = 5


class RealtimeLatencyStrategy(Strategy):
    """
    实时延迟测试策略 - 立即显示结果
    """
    
    def __init__(self, config: RealtimeLatencyConfig) -> None:
        super().__init__(config)
        self._test_config = config
        
        # 延迟数据存储
        self.latencies = defaultdict(lambda: defaultdict(list))
        self.recent_latencies = defaultdict(lambda: defaultdict(lambda: deque(maxlen=50)))
        self.message_counts = defaultdict(lambda: defaultdict(int))
        
        # 测试状态
        self._test_start_time = None
        self._total_messages = 0
        self._last_stat_time = 0

    def on_start(self) -> None:
        """策略启动"""
        flush_print("🎯 " + "="*60)
        flush_print("🎯 币安延迟测试启动 - 实时版本")
        flush_print("🎯 " + "="*60)
        
        self._test_start_time = time.time()
        
        flush_print(f"📊 测试币种: {', '.join(self._test_config.test_symbols)}")
        flush_print(f"⏱️  测试时长: {self._test_config.test_duration_minutes}分钟")
        flush_print(f"📈 订单簿深度: {self._test_config.orderbook_depth}档")
        flush_print()
        
        # 订阅数据
        for symbol in self._test_config.test_symbols:
            try:
                # 现货
                spot_id = InstrumentId.from_str(f"{symbol}USDT.BINANCE")
                self.subscribe_order_book_deltas(spot_id, BookType.L2_MBP, depth=5)
                flush_print(f"✅ {symbol} 现货已订阅")
                
                # 合约
                futures_id = InstrumentId.from_str(f"{symbol}USDT-PERP.BINANCE")
                self.subscribe_order_book_deltas(futures_id, BookType.L2_MBP, depth=5)
                flush_print(f"✅ {symbol} 合约已订阅")
                
            except Exception as e:
                flush_print(f"❌ {symbol} 订阅失败: {e}")
        
        flush_print("\n🚀 开始接收数据...")
        flush_print("📊 实时延迟统计将每收到100条消息显示一次")
        flush_print("-" * 60)
        
        # 设置定时器
        self.clock.set_timer("stats", timedelta(seconds=5), self.show_realtime_stats)
        self.clock.set_timer("end", timedelta(minutes=self._test_config.test_duration_minutes), self.end_test)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """处理orderbook数据 - 立即显示"""
        try:
            # 计算延迟
            current_time_ns = time.time_ns()
            latency_ms = (current_time_ns - deltas.ts_event) / 1_000_000
            
            # 解析交易对
            instrument_str = str(deltas.instrument_id)
            symbol, market_type = self._parse_instrument(instrument_str)
            
            if symbol and symbol in self._test_config.test_symbols:
                # 存储数据
                self.latencies[symbol][market_type].append(latency_ms)
                self.recent_latencies[symbol][market_type].append(latency_ms)
                self.message_counts[symbol][market_type] += 1
                self._total_messages += 1
                
                # 每收到50条消息显示一次快速统计
                if self._total_messages % 50 == 0:
                    market_name = "现货" if market_type == "spot" else "合约"
                    recent_avg = statistics.mean(list(self.recent_latencies[symbol][market_type])[-10:]) if len(self.recent_latencies[symbol][market_type]) >= 10 else latency_ms
                    flush_print(f"📊 {symbol} {market_name}: 消息#{self._total_messages} 延迟={latency_ms:.1f}ms 近期平均={recent_avg:.1f}ms")
                
        except Exception as e:
            flush_print(f"⚠️  数据处理错误: {e}")

    def _parse_instrument(self, instrument_str: str):
        """解析交易对"""
        try:
            base_part = instrument_str.split('.')[0]
            if '-PERP' in base_part:
                symbol = base_part.replace('-PERP', '').replace('USDT', '')
                return symbol, "futures"
            else:
                symbol = base_part.replace('USDT', '')
                return symbol, "spot"
        except:
            return None, "unknown"

    def show_realtime_stats(self) -> None:
        """显示实时统计"""
        if not self._test_start_time:
            return
            
        elapsed = time.time() - self._test_start_time
        
        flush_print(f"\n⏰ 运行时间: {elapsed:.1f}秒 | 总消息: {self._total_messages:,}")
        flush_print("=" * 60)
        
        for symbol in self._test_config.test_symbols:
            flush_print(f"\n💰 {symbol}:")
            
            for market_type in ["spot", "futures"]:
                count = self.message_counts[symbol][market_type]
                recent_data = list(self.recent_latencies[symbol][market_type])
                
                if recent_data:
                    avg = statistics.mean(recent_data)
                    latest = recent_data[-1] if recent_data else 0
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    flush_print(f"  {market_emoji} {market_name}: 消息={count:,} | 平均={avg:.2f}ms | 最新={latest:.2f}ms")
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    flush_print(f"  {market_emoji} {market_name}: ⏳ 等待数据...")

    def end_test(self) -> None:
        """结束测试"""
        flush_print("\n🏁 " + "="*60)
        flush_print("🏁 测试完成！最终报告:")
        flush_print("🏁 " + "="*60)
        
        if not self._test_start_time:
            flush_print("❌ 测试未正常启动")
            self.stop()
            return
            
        total_duration = time.time() - self._test_start_time
        
        flush_print(f"⏱️  实际测试时长: {total_duration:.1f}秒")
        flush_print(f"📊 总消息数量: {self._total_messages:,}")
        
        # 生成最终统计
        all_latencies = []
        for symbol in self._test_config.test_symbols:
            flush_print(f"\n💰 {symbol} 最终统计:")
            
            for market_type in ["spot", "futures"]:
                latencies = self.latencies[symbol][market_type]
                count = self.message_counts[symbol][market_type]
                
                if latencies:
                    avg = statistics.mean(latencies)
                    median = statistics.median(latencies)
                    min_lat = min(latencies)
                    max_lat = max(latencies)
                    
                    all_latencies.extend(latencies)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    
                    flush_print(f"  {market_emoji} {market_name}:")
                    flush_print(f"    📊 消息数量: {count:,}")
                    flush_print(f"    ⚡ 平均延迟: {avg:.3f}ms")
                    flush_print(f"    🎯 中位延迟: {median:.3f}ms")
                    flush_print(f"    📏 延迟范围: {min_lat:.3f} - {max_lat:.3f}ms")
                    
                    # 延迟分布
                    fast = sum(1 for l in latencies if l < 10)
                    medium = sum(1 for l in latencies if 10 <= l < 50)
                    slow = sum(1 for l in latencies if l >= 50)
                    flush_print(f"    📈 分布: <10ms({fast/count*100:.1f}%) 10-50ms({medium/count*100:.1f}%) >=50ms({slow/count*100:.1f}%)")
        
        # 全局统计
        if all_latencies:
            flush_print(f"\n🌍 全局延迟统计:")
            flush_print(f"   平均延迟: {statistics.mean(all_latencies):.3f}ms")
            flush_print(f"   中位延迟: {statistics.median(all_latencies):.3f}ms")
            flush_print(f"   总样本数: {len(all_latencies):,}")
        
        # 保存报告
        self._save_report(total_duration, all_latencies)
        
        flush_print("\n🎉 测试完成！")
        self.stop()

    def _save_report(self, duration: float, all_latencies: List[float]):
        """保存测试报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_latency_realtime_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("币安Orderbook延迟测试报告 (实时版本)\n")
                f.write("="*50 + "\n")
                f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"测试时长: {duration:.1f}秒\n")
                f.write(f"总消息数: {self._total_messages:,}\n")
                f.write(f"API密钥: {os.environ.get('BINANCE_API_KEY', 'N/A')[:10]}...\n")
                f.write("\n")
                
                for symbol in self._test_config.test_symbols:
                    f.write(f"{symbol} 统计:\n")
                    for market_type in ["spot", "futures"]:
                        latencies = self.latencies[symbol][market_type]
                        count = self.message_counts[symbol][market_type]
                        if latencies:
                            avg = statistics.mean(latencies)
                            market_name = "现货" if market_type == "spot" else "合约"
                            f.write(f"  {market_name}: 消息={count}, 平均延迟={avg:.2f}ms\n")
                    f.write("\n")
                
                if all_latencies:
                    f.write(f"全局平均延迟: {statistics.mean(all_latencies):.3f}ms\n")
            
            flush_print(f"📄 报告已保存: {filename}")
            
        except Exception as e:
            flush_print(f"❌ 保存报告失败: {e}")


def main():
    """主函数 - 优化输出"""
    import os
    # 设置无缓冲输出
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    
    flush_print("🚀 " + "="*70)
    flush_print("🚀 NautilusTrader 币安延迟测试 - 实时版本")
    flush_print("🚀 " + "="*70)
    
    try:
        api_key = os.environ.get('BINANCE_API_KEY', '')
        api_secret = os.environ.get('BINANCE_API_SECRET', '')
        
        if not api_key or not api_secret:
            flush_print("❌ 缺少API凭证")
            return
        
        flush_print(f"✅ API密钥: {api_key[:10]}...")
        flush_print("🔧 创建交易节点...")
        
        # 简化配置，关闭仪器自动加载
        config = TradingNodeConfig(
            trader_id=TraderId("REALTIME-LATENCY-TESTER"),
            logging=LoggingConfig(
                log_level="WARN",  # 减少日志输出
                use_pyo3=True,
                log_colors=False,
            ),
            data_clients={
                BINANCE: BinanceDataClientConfig(
                    api_key=api_key,
                    api_secret=api_secret,
                    account_type=BinanceAccountType.SPOT,
                    testnet=False,
                    instrument_provider=InstrumentProviderConfig(load_all=False),  # 关闭自动加载
                ),
            },
            timeout_connection=10.0,
        )
        
        node = TradingNode(config=config)
        strategy = RealtimeLatencyStrategy(config=RealtimeLatencyConfig())
        
        node.trader.add_strategy(strategy)
        node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
        
        flush_print("🔧 构建节点...")
        node.build()
        
        flush_print("🚀 启动测试...")
        flush_print("💡 每收到50条消息会显示进度")
        flush_print("⏱️  每5秒显示详细统计")
        flush_print()
        
        # 运行测试
        node.run()
        
    except KeyboardInterrupt:
        flush_print("\n⏹️  测试被中断")
    except Exception as e:
        flush_print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            if 'node' in locals():
                node.dispose()
        except:
            pass
        flush_print("👋 测试结束")


if __name__ == "__main__":
    import os
    main()
