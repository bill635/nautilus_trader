#!/usr/bin/env python3
"""
最简单的币安延迟测试 - 避免所有配置问题
"""

import time
import statistics
import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta

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


class SimpleLatencyConfig(StrategyConfig, frozen=True):
    """最简单的配置"""
    pass


class SimpleLatencyStrategy(Strategy):
    """最简单的延迟测试策略"""
    
    def __init__(self, config: SimpleLatencyConfig) -> None:
        super().__init__(config)
        
        # 测试币种 - 硬编码避免配置问题
        self.test_symbols = ["API3", "OM", "CFX"]
        
        # 数据存储
        self.latencies = defaultdict(list)
        self.message_counts = defaultdict(int)
        self.start_time = None
        self.message_counter = 0

    def on_start(self) -> None:
        """启动策略"""
        print("🎯 开始币安延迟测试！", flush=True)
        print("测试币种: API3, OM, CFX", flush=True)
        print("="*50, flush=True)
        
        self.start_time = time.time()
        
        # 订阅数据
        for symbol in self.test_symbols:
            try:
                # 现货
                spot_id = InstrumentId.from_str(f"{symbol}USDT.BINANCE")
                self.subscribe_order_book_deltas(spot_id, BookType.L2_MBP, depth=5)
                print(f"✅ {symbol} 现货", flush=True)
                
                # 合约
                futures_id = InstrumentId.from_str(f"{symbol}USDT-PERP.BINANCE")
                self.subscribe_order_book_deltas(futures_id, BookType.L2_MBP, depth=5)
                print(f"✅ {symbol} 合约", flush=True)
                
            except Exception as e:
                print(f"❌ {symbol} 失败: {e}", flush=True)
        
        print("\n🚀 开始接收数据...", flush=True)
        
        # 设置定时器
        self.clock.set_timer("stats", timedelta(seconds=5), self.print_stats)
        self.clock.set_timer("end", timedelta(minutes=1), self.end_test)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """处理数据"""
        try:
            # 计算延迟
            latency_ms = (time.time_ns() - deltas.ts_event) / 1_000_000
            
            # 获取交易对信息
            instrument_str = str(deltas.instrument_id)
            
            # 存储数据
            self.latencies[instrument_str].append(latency_ms)
            self.message_counts[instrument_str] += 1
            self.message_counter += 1
            
            # 每20条消息显示进度
            if self.message_counter % 20 == 0:
                elapsed = time.time() - self.start_time
                symbol_short = instrument_str.split('.')[0]
                print(f"📊 #{self.message_counter} {symbol_short}: {latency_ms:.1f}ms (运行{elapsed:.0f}s)", flush=True)
                
        except Exception as e:
            print(f"❌ 数据错误: {e}", flush=True)

    def print_stats(self) -> None:
        """打印统计"""
        if not self.start_time:
            return
            
        elapsed = time.time() - self.start_time
        print(f"\n📈 统计 - 运行{elapsed:.0f}秒 | 总消息: {self.message_counter}", flush=True)
        print("="*60, flush=True)
        
        # 按币种分组统计
        symbol_stats = defaultdict(lambda: {"spot": [], "futures": []})
        
        for instrument_str, latencies in self.latencies.items():
            if latencies:
                # 解析币种和类型
                base_part = instrument_str.split('.')[0]
                if '-PERP' in base_part:
                    symbol = base_part.replace('-PERP', '').replace('USDT', '')
                    market_type = "futures"
                else:
                    symbol = base_part.replace('USDT', '')
                    market_type = "spot"
                
                if symbol in self.test_symbols:
                    symbol_stats[symbol][market_type] = latencies[-20:]  # 最近20个
        
        # 显示统计
        for symbol in self.test_symbols:
            print(f"\n💰 {symbol}:", flush=True)
            
            spot_data = symbol_stats[symbol]["spot"]
            futures_data = symbol_stats[symbol]["futures"]
            
            if spot_data:
                avg = statistics.mean(spot_data)
                count = len([k for k in self.latencies.keys() if symbol in k and 'PERP' not in k])
                print(f"  🏪 现货: 平均={avg:.2f}ms", flush=True)
            
            if futures_data:
                avg = statistics.mean(futures_data)
                count = len([k for k in self.latencies.keys() if symbol in k and 'PERP' in k])
                print(f"  📈 合约: 平均={avg:.2f}ms", flush=True)

    def end_test(self) -> None:
        """结束测试"""
        print("\n🏁 测试完成！最终报告:", flush=True)
        print("="*60, flush=True)
        
        if not self.start_time:
            print("❌ 测试未正常启动", flush=True)
            self.stop()
            return
        
        total_duration = time.time() - self.start_time
        print(f"⏱️  测试时长: {total_duration:.1f}秒", flush=True)
        print(f"📊 总消息: {self.message_counter:,}", flush=True)
        
        # 按币种汇总
        symbol_results = defaultdict(lambda: {"spot": [], "futures": []})
        
        for instrument_str, latencies in self.latencies.items():
            base_part = instrument_str.split('.')[0]
            if '-PERP' in base_part:
                symbol = base_part.replace('-PERP', '').replace('USDT', '')
                symbol_results[symbol]["futures"] = latencies
            else:
                symbol = base_part.replace('USDT', '')
                symbol_results[symbol]["spot"] = latencies
        
        # 显示最终结果
        all_latencies = []
        for symbol in self.test_symbols:
            print(f"\n💰 {symbol} 最终结果:", flush=True)
            
            spot_latencies = symbol_results[symbol]["spot"]
            futures_latencies = symbol_results[symbol]["futures"]
            
            if spot_latencies:
                avg = statistics.mean(spot_latencies)
                median = statistics.median(spot_latencies)
                count = len(spot_latencies)
                all_latencies.extend(spot_latencies)
                print(f"  🏪 现货: {count:,}条消息, 平均={avg:.3f}ms, 中位={median:.3f}ms", flush=True)
            
            if futures_latencies:
                avg = statistics.mean(futures_latencies)
                median = statistics.median(futures_latencies)
                count = len(futures_latencies)
                all_latencies.extend(futures_latencies)
                print(f"  📈 合约: {count:,}条消息, 平均={avg:.3f}ms, 中位={median:.3f}ms", flush=True)
        
        # 全局统计
        if all_latencies:
            global_avg = statistics.mean(all_latencies)
            global_median = statistics.median(all_latencies)
            print(f"\n🌍 全局统计:", flush=True)
            print(f"   平均延迟: {global_avg:.3f}ms", flush=True)
            print(f"   中位延迟: {global_median:.3f}ms", flush=True)
            print(f"   样本总数: {len(all_latencies):,}", flush=True)
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_simple_{timestamp}.txt"
        
        try:
            with open(filename, 'w') as f:
                f.write(f"币安延迟测试简单报告 - {datetime.now()}\n")
                f.write(f"测试时长: {total_duration:.1f}秒\n")
                f.write(f"总消息: {self.message_counter}\n")
                f.write(f"全局平均延迟: {global_avg:.3f}ms\n" if all_latencies else "未收到数据\n")
                f.write("\n各币种详情:\n")
                for symbol in self.test_symbols:
                    spot_latencies = symbol_results[symbol]["spot"]
                    futures_latencies = symbol_results[symbol]["futures"]
                    if spot_latencies:
                        f.write(f"{symbol} 现货: {len(spot_latencies)}条, 平均{statistics.mean(spot_latencies):.2f}ms\n")
                    if futures_latencies:
                        f.write(f"{symbol} 合约: {len(futures_latencies)}条, 平均{statistics.mean(futures_latencies):.2f}ms\n")
            
            print(f"📄 报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n🎉 测试完成！", flush=True)
        self.stop()


def main():
    """主函数"""
    print("🚀 币安延迟测试启动", flush=True)
    
    # 检查API凭证
    api_key = os.environ.get('BINANCE_API_KEY', '')
    api_secret = os.environ.get('BINANCE_API_SECRET', '')
    
    if not api_key or not api_secret:
        print("❌ 需要币安API凭证", flush=True)
        return
    
    print(f"✅ API密钥验证通过", flush=True)
    
    try:
        # 最简单的配置
        config = TradingNodeConfig(
            trader_id=TraderId("SIMPLE-TEST"),
            logging=LoggingConfig(log_level="ERROR"),
            data_clients={
                BINANCE: BinanceDataClientConfig(
                    api_key=api_key,
                    api_secret=api_secret,
                    account_type=BinanceAccountType.SPOT,
                    testnet=False,
                    instrument_provider=InstrumentProviderConfig(load_all=False),
                ),
            },
        )
        
        node = TradingNode(config=config)
        strategy = SimpleLatencyStrategy(config=SimpleLatencyConfig())
        
        node.trader.add_strategy(strategy)
        node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
        
        node.build()
        node.run()
        
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
