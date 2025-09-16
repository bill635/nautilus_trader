#!/usr/bin/env python3
"""
币安Orderbook延迟测试 - 修复版本
解决预编译版本的API兼容性问题
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
from nautilus_trader.common.events import TimeEvent


class FixedLatencyConfig(StrategyConfig, frozen=True):
    """修复版延迟测试配置"""
    pass


class FixedLatencyStrategy(Strategy):
    """修复版延迟测试策略 - 兼容预编译版本"""
    
    def __init__(self, config: FixedLatencyConfig) -> None:
        super().__init__(config)
        
        # 测试币种 - 硬编码避免配置问题
        self.test_symbols = ["API3", "OM", "CFX"]
        
        # 数据存储
        self.latencies = defaultdict(list)
        self.message_counts = defaultdict(int)
        self.start_time = None
        self.message_counter = 0
        self.test_start_timestamp = None

    def on_start(self) -> None:
        """启动策略 - 修复定时器API"""
        print("🎯 开始币安延迟测试！(修复版)", flush=True)
        print("测试币种: API3, OM, CFX", flush=True)
        print("="*50, flush=True)
        
        self.start_time = time.time()
        self.test_start_timestamp = datetime.utcnow()
        
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
        
        # 修复定时器API - 使用正确的回调签名
        try:
            self.clock.set_timer(
                name="stats_timer",
                interval=timedelta(seconds=5),
                callback=self.on_stats_timer,  # 修复：使用正确的回调方法
            )
            
            self.clock.set_timer(
                name="end_timer", 
                interval=timedelta(minutes=1),
                callback=self.on_end_timer,  # 修复：使用正确的回调方法
            )
            
            print("✅ 定时器设置成功", flush=True)
            
        except Exception as e:
            print(f"❌ 定时器设置失败: {e}", flush=True)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """处理orderbook数据"""
        try:
            # 计算延迟
            latency_ms = (time.time_ns() - deltas.ts_event) / 1_000_000
            
            # 获取交易对信息
            instrument_str = str(deltas.instrument_id)
            
            # 存储数据
            self.latencies[instrument_str].append(latency_ms)
            self.message_counts[instrument_str] += 1
            self.message_counter += 1
            
            # 每10条消息显示进度
            if self.message_counter % 10 == 0:
                elapsed = time.time() - self.start_time
                symbol_short = instrument_str.split('.')[0]
                print(f"📊 #{self.message_counter} {symbol_short}: {latency_ms:.1f}ms (运行{elapsed:.0f}s)", flush=True)
                
        except Exception as e:
            print(f"❌ 数据处理错误: {e}", flush=True)

    def on_stats_timer(self, event: TimeEvent) -> None:
        """定时器回调 - 打印统计信息"""
        try:
            if not self.start_time:
                return
                
            elapsed = time.time() - self.start_time
            print(f"\n📈 定时统计 - 运行{elapsed:.0f}秒 | 总消息: {self.message_counter}", flush=True)
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
                        symbol_stats[symbol][market_type] = latencies[-10:]  # 最近10个
            
            # 显示统计
            for symbol in self.test_symbols:
                print(f"\n💰 {symbol}:", flush=True)
                
                spot_data = symbol_stats[symbol]["spot"]
                futures_data = symbol_stats[symbol]["futures"]
                
                if spot_data:
                    avg = statistics.mean(spot_data)
                    latest = spot_data[-1]
                    print(f"  🏪 现货: 平均={avg:.2f}ms, 最新={latest:.2f}ms", flush=True)
                else:
                    print(f"  🏪 现货: ⏳ 等待数据...", flush=True)
                
                if futures_data:
                    avg = statistics.mean(futures_data)
                    latest = futures_data[-1]
                    print(f"  📈 合约: 平均={avg:.2f}ms, 最新={latest:.2f}ms", flush=True)
                else:
                    print(f"  📈 合约: ⏳ 等待数据...", flush=True)
                    
        except Exception as e:
            print(f"❌ 统计错误: {e}", flush=True)

    def on_end_timer(self, event: TimeEvent) -> None:
        """结束定时器回调"""
        try:
            print("\n🏁 测试时间到！生成最终报告...", flush=True)
            self.generate_final_report()
            self.stop()
        except Exception as e:
            print(f"❌ 结束测试错误: {e}", flush=True)

    def generate_final_report(self):
        """生成最终报告"""
        print("="*70, flush=True)
        print("🏁 币安延迟测试最终报告", flush=True)
        print("="*70, flush=True)
        
        if not self.start_time:
            print("❌ 测试未正常启动", flush=True)
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
        report_lines = []
        report_lines.append("币安延迟测试最终报告 (修复版)")
        report_lines.append("="*50)
        report_lines.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"测试时长: {total_duration:.1f}秒")
        report_lines.append(f"总消息: {self.message_counter}")
        report_lines.append("")
        
        for symbol in self.test_symbols:
            print(f"\n💰 {symbol} 最终结果:", flush=True)
            report_lines.append(f"{symbol}:")
            
            spot_latencies = symbol_results[symbol]["spot"]
            futures_latencies = symbol_results[symbol]["futures"]
            
            if spot_latencies:
                avg = statistics.mean(spot_latencies)
                median = statistics.median(spot_latencies)
                count = len(spot_latencies)
                min_lat = min(spot_latencies)
                max_lat = max(spot_latencies)
                all_latencies.extend(spot_latencies)
                
                print(f"  🏪 现货: {count:,}条消息", flush=True)
                print(f"      平均={avg:.3f}ms, 中位={median:.3f}ms", flush=True)
                print(f"      范围={min_lat:.3f}-{max_lat:.3f}ms", flush=True)
                
                report_lines.append(f"  现货: {count}条, 平均{avg:.2f}ms, 中位{median:.2f}ms")
            else:
                print(f"  🏪 现货: ❌ 未收到数据", flush=True)
                report_lines.append("  现货: 未收到数据")
            
            if futures_latencies:
                avg = statistics.mean(futures_latencies)
                median = statistics.median(futures_latencies)
                count = len(futures_latencies)
                min_lat = min(futures_latencies)
                max_lat = max(futures_latencies)
                all_latencies.extend(futures_latencies)
                
                print(f"  📈 合约: {count:,}条消息", flush=True)
                print(f"      平均={avg:.3f}ms, 中位={median:.3f}ms", flush=True)
                print(f"      范围={min_lat:.3f}-{max_lat:.3f}ms", flush=True)
                
                report_lines.append(f"  合约: {count}条, 平均{avg:.2f}ms, 中位{median:.2f}ms")
            else:
                print(f"  📈 合约: ❌ 未收到数据", flush=True)
                report_lines.append("  合约: 未收到数据")
            
            report_lines.append("")
        
        # 全局统计
        if all_latencies:
            global_avg = statistics.mean(all_latencies)
            global_median = statistics.median(all_latencies)
            
            print(f"\n🌍 全局统计:", flush=True)
            print(f"   平均延迟: {global_avg:.3f}ms", flush=True)
            print(f"   中位延迟: {global_median:.3f}ms", flush=True)
            print(f"   样本总数: {len(all_latencies):,}", flush=True)
            
            report_lines.append("全局统计:")
            report_lines.append(f"  平均延迟: {global_avg:.3f}ms")
            report_lines.append(f"  中位延迟: {global_median:.3f}ms")
            report_lines.append(f"  样本总数: {len(all_latencies):,}")
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_fixed_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n🎉 测试完成！", flush=True)


def main():
    """主函数"""
    print("🚀 币安延迟测试启动 (修复版)", flush=True)
    
    # 检查API凭证
    api_key = os.environ.get('BINANCE_API_KEY', '')
    api_secret = os.environ.get('BINANCE_API_SECRET', '')
    
    if not api_key or not api_secret:
        print("❌ 需要币安API凭证", flush=True)
        print("💡 设置环境变量 BINANCE_API_KEY 和 BINANCE_API_SECRET", flush=True)
        return
    
    print(f"✅ API密钥: {api_key[:10]}...", flush=True)
    
    try:
        # 简化配置 - 关闭自动加载避免仪器问题
        config = TradingNodeConfig(
            trader_id=TraderId("FIXED-LATENCY-TEST"),
            logging=LoggingConfig(
                log_level="WARN",  # 减少日志噪音
                use_pyo3=True,
                log_colors=False,
            ),
            data_clients={
                BINANCE: BinanceDataClientConfig(
                    api_key=api_key,
                    api_secret=api_secret,
                    account_type=BinanceAccountType.SPOT,
                    testnet=False,
                    instrument_provider=InstrumentProviderConfig(load_all=False),  # 关键：不自动加载所有仪器
                ),
            },
            timeout_connection=15.0,
        )
        
        print("🏗️  创建交易节点...", flush=True)
        node = TradingNode(config=config)
        
        strategy = FixedLatencyStrategy(config=FixedLatencyConfig())
        node.trader.add_strategy(strategy)
        node.add_data_client_factory(BINANCE, BinanceLiveDataClientFactory)
        
        print("🔧 构建节点...", flush=True)
        node.build()
        
        print("🚀 启动测试...", flush=True)
        print("💡 每10条消息显示进度，每5秒显示统计", flush=True)
        print("⏱️  测试将运行1分钟", flush=True)
        print("-"*50, flush=True)
        
        node.run()
        
    except KeyboardInterrupt:
        print("\n⏹️  测试被中断", flush=True)
    except Exception as e:
        print(f"❌ 测试错误: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        try:
            if 'node' in locals():
                node.dispose()
        except:
            pass
        print("👋 清理完成", flush=True)


if __name__ == "__main__":
    main()
