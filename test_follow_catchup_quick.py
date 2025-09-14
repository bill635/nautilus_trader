#!/usr/bin/env python3
"""
快速测试版本 - Follow-Catchup套利策略
运行30秒，快速验证功能是否正常
"""

import asyncio
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, deque
from datetime import datetime

try:
    import ccxt
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt pandas numpy"])
    import ccxt
    import pandas as pd
    import numpy as np


class QuickTestEngine:
    """快速测试引擎"""
    
    def __init__(self):
        # 简化配置
        self.tokens = ["API3", "OM", "CFX"]
        self.test_duration = 30  # 30秒快速测试
        
        # 策略参数
        self.window_ms = 200
        self.move_thr_bps = 50.0  # 降低阈值，更容易触发
        self.spread_thr_bps = 5.0  # 降低价差阈值
        
        # 交易所配置
        self.exchanges = {}
        self.running = False
        self.start_time = None
        
        # 数据存储
        self.price_data = defaultdict(lambda: deque(maxlen=100))
        self.events = []
        self.data_counts = defaultdict(int)

    async def initialize(self):
        """初始化"""
        print("🚀 快速测试初始化...", flush=True)
        
        try:
            # 只初始化币安（简化测试）
            self.exchanges['binance_spot'] = ccxt.binance({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'spot'}
            })
            
            self.exchanges['binance_perp'] = ccxt.binance({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'future'}
            })
            
            print("✅ 币安交易所初始化成功", flush=True)
            self.running = True
            
        except Exception as e:
            print(f"❌ 初始化失败: {e}", flush=True)
            raise

    async def collect_data(self, token: str, exchange_key: str):
        """收集数据"""
        symbol = f"{token}/USDT"
        exchange = self.exchanges[exchange_key]
        
        while self.running:
            try:
                ticker = exchange.fetch_ticker(symbol)
                if ticker and ticker['last']:
                    timestamp_ns = int(time.time_ns())
                    price = float(ticker['last'])
                    
                    key = f"{exchange_key}_{token}"
                    self.price_data[key].append((timestamp_ns, price))
                    self.data_counts[key] += 1
                
                await asyncio.sleep(0.2)  # 200ms间隔
                
            except Exception as e:
                await asyncio.sleep(1)  # 错误时等待1秒

    async def detect_events(self):
        """事件检测"""
        await asyncio.sleep(5)  # 等待数据积累
        
        while self.running:
            try:
                current_time_ns = int(time.time_ns())
                window_ns = int(self.window_ms) * 1_000_000
                
                for token in self.tokens:
                    # 检查该币种的价格移动
                    for exchange_key in ['binance_spot', 'binance_perp']:
                        key = f"{exchange_key}_{token}"
                        
                        if key in self.price_data and len(self.price_data[key]) >= 5:
                            data = list(self.price_data[key])
                            
                            # 获取最新和基准价格
                            latest_ts, latest_px = data[-1]
                            base_ts = latest_ts - window_ns
                            
                            # 找到基准价格
                            base_px = None
                            for ts, px in reversed(data):
                                if ts <= base_ts:
                                    base_px = px
                                    break
                            
                            if base_px is not None:
                                ret_bps = (latest_px / base_px - 1.0) * 10000
                                
                                # 检查移动阈值
                                if abs(ret_bps) >= self.move_thr_bps:
                                    await self._create_event(token, key, ret_bps, latest_ts)
                
                await asyncio.sleep(0.1)  # 100ms检测间隔
                
            except Exception as e:
                await asyncio.sleep(0.5)

    async def _create_event(self, token: str, source: str, ret_bps: float, timestamp_ns: int):
        """创建事件"""
        try:
            event = {
                'timestamp': pd.to_datetime(timestamp_ns, unit='ns', utc=True),
                'token': token,
                'source': source,
                'ret_bps': ret_bps
            }
            
            self.events.append(event)
            
            elapsed = (time.time_ns() - int(self.start_time * 1e9)) / 1e9
            print(f"🚨 事件#{len(self.events)} {token} {source}: {ret_bps:.1f}bps (运行{elapsed:.0f}s)", flush=True)
            
        except Exception as e:
            pass

    async def print_stats(self):
        """打印统计"""
        while self.running:
            await asyncio.sleep(10)  # 每10秒
            
            if self.start_time:
                elapsed = time.time() - self.start_time
                total_data = sum(self.data_counts.values())
                
                print(f"\n📊 快速测试统计 - 运行{elapsed:.0f}秒", flush=True)
                print(f"📡 总数据点: {total_data:,}", flush=True)
                print(f"🚨 检测事件: {len(self.events):,}", flush=True)
                
                print("\n💰 各币种数据收集:", flush=True)
                for token in self.tokens:
                    spot_key = f"binance_spot_{token}"
                    perp_key = f"binance_perp_{token}"
                    
                    spot_count = self.data_counts[spot_key]
                    perp_count = self.data_counts[perp_key]
                    
                    print(f"  {token}: 现货={spot_count:,}, 合约={perp_count:,}", flush=True)
                
                if self.events:
                    print(f"\n🚨 最近事件:", flush=True)
                    for event in self.events[-3:]:  # 显示最近3个事件
                        print(f"  {event['token']} {event['source']}: {event['ret_bps']:.1f}bps", flush=True)

    async def run_quick_test(self):
        """运行快速测试"""
        print("🚀 " + "="*60, flush=True)
        print("🚀 Follow-Catchup策略快速测试 (30秒)", flush=True)
        print("🚀 " + "="*60, flush=True)
        
        await self.initialize()
        
        self.start_time = time.time()
        
        # 创建任务
        tasks = []
        
        # 数据收集任务
        for token in self.tokens:
            tasks.append(asyncio.create_task(
                self.collect_data(token, "binance_spot"),
                name=f"{token}_spot"
            ))
            tasks.append(asyncio.create_task(
                self.collect_data(token, "binance_perp"),
                name=f"{token}_perp"
            ))
        
        # 事件检测任务
        tasks.append(asyncio.create_task(self.detect_events(), name="events"))
        
        # 统计任务
        tasks.append(asyncio.create_task(self.print_stats(), name="stats"))
        
        # 定时停止
        async def stop_test():
            await asyncio.sleep(self.test_duration)
            self.running = False
            print(f"\n⏰ {self.test_duration}秒测试完成", flush=True)
        
        tasks.append(asyncio.create_task(stop_test(), name="timer"))
        
        print("🚀 开始快速测试...", flush=True)
        print("💡 数据收集中，请等待事件检测...", flush=True)
        print("-" * 60, flush=True)
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            print(f"❌ 测试错误: {e}", flush=True)
        finally:
            await self.cleanup()

    async def cleanup(self):
        """清理资源"""
        self.running = False
        
        # 生成简单报告
        print(f"\n🏁 快速测试完成报告", flush=True)
        print("="*50, flush=True)
        
        if self.start_time:
            duration = time.time() - self.start_time
            total_data = sum(self.data_counts.values())
            
            print(f"⏱️  测试时长: {duration:.1f}秒", flush=True)
            print(f"📡 数据收集: {total_data:,}个数据点", flush=True)
            print(f"🚨 事件检测: {len(self.events):,}个事件", flush=True)
            
            if total_data > 0:
                print(f"📈 数据频率: {total_data/duration:.1f}点/秒", flush=True)
            
            # 保存简单报告
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reports/quick_test_report_{timestamp}.txt"
            
            try:
                with open(filename, 'w') as f:
                    f.write(f"Follow-Catchup策略快速测试报告\n")
                    f.write(f"测试时间: {datetime.now()}\n")
                    f.write(f"测试时长: {duration:.1f}秒\n")
                    f.write(f"数据点数: {total_data}\n")
                    f.write(f"检测事件: {len(self.events)}\n")
                    f.write(f"策略状态: {'正常运行' if total_data > 50 else '数据不足'}\n")
                
                print(f"📄 报告保存: {filename}", flush=True)
            except Exception as e:
                print(f"❌ 保存失败: {e}", flush=True)
        
        # 关闭交易所连接
        for ex in self.exchanges.values():
            try:
                if hasattr(ex, 'close'):
                    await ex.close()
            except:
                pass
        
        print("✅ 清理完成", flush=True)


async def main():
    """主函数"""
    print("🚀 Follow-Catchup策略快速验证测试", flush=True)
    
    engine = QuickTestEngine()
    
    try:
        await engine.run_quick_test()
    except KeyboardInterrupt:
        print("\n⏹️  测试被中断", flush=True)
    except Exception as e:
        print(f"❌ 测试失败: {e}", flush=True)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import os
    os.makedirs("reports", exist_ok=True)
    
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ 启动失败: {e}", flush=True)
    finally:
        print("👋 测试结束", flush=True)
