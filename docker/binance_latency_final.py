#!/usr/bin/env python3
"""
币安Orderbook延迟测试 - 最终版本
简化配置，立即显示结果
"""

import time
import statistics
import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List
from dataclasses import field

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


class FinalLatencyConfig(StrategyConfig, frozen=True):
    """最终版延迟测试配置"""
    test_symbols: List[str] = field(default_factory=lambda: ["API3", "OM", "CFX"])


class FinalLatencyStrategy(Strategy):
    """最终版延迟测试策略"""
    
    def __init__(self, config: FinalLatencyConfig) -> None:
        super().__init__(config)
        self._test_config = config
        self.latencies = defaultdict(list)
        self.message_counts = defaultdict(int)
        self._start_time = None
        self._message_counter = 0

    def on_start(self) -> None:
        """启动策略"""
        print("🎯 币安延迟测试启动！", flush=True)
        print("="*50, flush=True)
        self._start_time = time.time()
        
        # 订阅数据
        for symbol in self._test_config.test_symbols:
            # 现货
            spot_id = InstrumentId.from_str(f"{symbol}USDT.BINANCE")
            self.subscribe_order_book_deltas(spot_id, BookType.L2_MBP, depth=5)
            print(f"✅ {symbol} 现货订阅", flush=True)
            
            # 合约
            futures_id = InstrumentId.from_str(f"{symbol}USDT-PERP.BINANCE")
            self.subscribe_order_book_deltas(futures_id, BookType.L2_MBP, depth=5)
            print(f"✅ {symbol} 合约订阅", flush=True)
        
        print("\n🚀 开始收集数据...", flush=True)
        
        # 设置定时器
        self.clock.set_timer("stats", timedelta(seconds=5), self.print_stats)
        self.clock.set_timer("end", timedelta(minutes=1), self.end_test)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """处理数据"""
        try:
            # 计算延迟
            latency_ms = (time.time_ns() - deltas.ts_event) / 1_000_000
            
            # 解析交易对
            instrument_str = str(deltas.instrument_id)
            symbol, market_type = self._parse_instrument(instrument_str)
            
            if symbol in self._test_config.test_symbols:
                key = f"{symbol}_{market_type}"
                self.latencies[key].append(latency_ms)
                self.message_counts[key] += 1
                self._message_counter += 1
                
                # 每收到20条消息显示一次进度
                if self._message_counter % 20 == 0:
                    elapsed = time.time() - self._start_time
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"📊 #{self._message_counter} {symbol} {market_name}: {latency_ms:.1f}ms (运行{elapsed:.0f}s)", flush=True)
                
        except Exception as e:
            print(f"❌ 处理数据错误: {e}", flush=True)

    def _parse_instrument(self, instrument_str: str):
        """解析交易对"""
        base_part = instrument_str.split('.')[0]
        if '-PERP' in base_part:
            return base_part.replace('-PERP', '').replace('USDT', ''), "futures"
        else:
            return base_part.replace('USDT', ''), "spot"

    def print_stats(self) -> None:
        """打印统计"""
        if not self._start_time:
            return
            
        elapsed = time.time() - self._start_time
        print(f"\n📈 延迟统计 - 运行{elapsed:.0f}秒 | 总消息: {self._message_counter}", flush=True)
        print("="*50, flush=True)
        
        for symbol in self._test_config.test_symbols:
            print(f"\n💰 {symbol}:", flush=True)
            
            for market_type in ["spot", "futures"]:
                key = f"{symbol}_{market_type}"
                if key in self.latencies and self.latencies[key]:
                    recent = self.latencies[key][-20:]  # 最近20个
                    avg = statistics.mean(recent)
                    count = self.message_counts[key]
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_name}: 消息={count:,}, 平均={avg:.2f}ms", flush=True)

    def end_test(self) -> None:
        """结束测试"""
        print("\n🏁 测试完成！", flush=True)
        print("="*60, flush=True)
        
        total_duration = time.time() - self._start_time
        print(f"⏱️  总时长: {total_duration:.1f}秒", flush=True)
        print(f"📊 总消息: {self._message_counter:,}", flush=True)
        
        # 最终统计
        for symbol in self._test_config.test_symbols:
            print(f"\n💰 {symbol} 最终结果:", flush=True)
            
            for market_type in ["spot", "futures"]:
                key = f"{symbol}_{market_type}"
                if key in self.latencies and self.latencies[key]:
                    latencies = self.latencies[key]
                    avg = statistics.mean(latencies)
                    median = statistics.median(latencies)
                    count = self.message_counts[key]
                    market_name = "现货" if market_type == "spot" else "合约"
                    
                    print(f"  {market_name}: 消息={count:,}, 平均={avg:.3f}ms, 中位={median:.3f}ms", flush=True)
        
        # 保存简单报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_final_{timestamp}.txt"
        
        try:
            with open(filename, 'w') as f:
                f.write(f"币安延迟测试报告 - {datetime.now()}\n")
                f.write(f"总消息: {self._message_counter}\n")
                for symbol in self._test_config.test_symbols:
                    for market_type in ["spot", "futures"]:
                        key = f"{symbol}_{market_type}"
                        if key in self.latencies:
                            avg = statistics.mean(self.latencies[key])
                            count = self.message_counts[key]
                            market_name = "现货" if market_type == "spot" else "合约"
                            f.write(f"{symbol} {market_name}: {count}条消息, 平均{avg:.2f}ms\n")
            print(f"📄 报告保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存失败: {e}", flush=True)
        
        self.stop()


def main():
    """主函数"""
    print("🚀 NautilusTrader 币安延迟测试", flush=True)
    print("="*50, flush=True)
    
    # 检查API凭证
    api_key = os.environ.get('BINANCE_API_KEY', '')
    api_secret = os.environ.get('BINANCE_API_SECRET', '')
    
    if not api_key or not api_secret:
        print("❌ 缺少币安API凭证", flush=True)
        return
    
    print(f"✅ API密钥: {api_key[:10]}...", flush=True)
    
    try:
        # 创建简化配置
        config = TradingNodeConfig(
            trader_id=TraderId("FINAL-LATENCY-TEST"),
            logging=LoggingConfig(log_level="ERROR"),  # 最小日志
            data_clients={
                BINANCE: BinanceDataClientConfig(
                    api_key=api_key,
                    api_secret=api_secret,
                    account_type=BinanceAccountType.SPOT,
                    testnet=False,
                    instrument_provider=InstrumentProviderConfig(load_all=False),  # 不加载所有仪器
                ),
            },
        )
        
        print("🔧 创建节点...", flush=True)
        node = TradingNode(config=config)
        
        strategy = FinalLatencyStrategy(config=FinalLatencyConfig())
        node.trader.add_strategy(strategy)
        node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
        
        print("🚀 启动测试...", flush=True)
        node.build()
        node.run()
        
    except KeyboardInterrupt:
        print("\n⏹️ 测试中断", flush=True)
    except Exception as e:
        print(f"❌ 错误: {e}", flush=True)
    finally:
        try:
            if 'node' in locals():
                node.dispose()
        except:
            pass


if __name__ == "__main__":
    main()
