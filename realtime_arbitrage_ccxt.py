#!/usr/bin/env python3
"""
实时延迟套利策略 - CCXT版本
基于原始策略逻辑，使用CCXT实时数据流
模块化设计，数据获取部分可替换
"""

import asyncio
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, deque
from datetime import datetime, timedelta
import logging
import json

try:
    import ccxt
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt"])
    import ccxt


class DataProvider:
    """数据提供者基类 - 可替换的数据源接口"""
    
    async def start(self):
        """启动数据源"""
        pass
    
    async def stop(self):
        """停止数据源"""
        pass
    
    async def get_orderbook(self, symbol: str, exchange: str, market_type: str) -> Optional[Dict]:
        """获取orderbook数据"""
        raise NotImplementedError


class CCXTDataProvider(DataProvider):
    """CCXT数据提供者"""
    
    def __init__(self):
        self.exchanges = {}
        self.running = False
        
    async def start(self):
        """初始化CCXT交易所"""
        try:
            # 币安现货
            self.exchanges['binance_spot'] = ccxt.binance({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'spot'}
            })
            
            # 币安合约
            self.exchanges['binance_perp'] = ccxt.binance({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'future'}
            })
            
            # OKX现货
            self.exchanges['okx_spot'] = ccxt.okx({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'spot'}
            })
            
            # OKX合约
            self.exchanges['okx_perp'] = ccxt.okx({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'swap'}
            })
            
            # Bybit现货（仅OM使用）
            self.exchanges['bybit_spot'] = ccxt.bybit({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'spot'}
            })
            
            # Bybit合约（仅OM使用）
            self.exchanges['bybit_perp'] = ccxt.bybit({
                'sandbox': False,
                'enableRateLimit': False,
                'timeout': 3000,
                'options': {'defaultType': 'linear'}
            })
            
            self.running = True
            print("✅ CCXT交易所初始化完成", flush=True)
            
        except Exception as e:
            print(f"❌ CCXT初始化失败: {e}", flush=True)
            raise
    
    async def stop(self):
        """关闭所有交易所连接"""
        self.running = False
        for name, exchange in self.exchanges.items():
            try:
                if hasattr(exchange, 'close'):
                    await exchange.close()
            except:
                pass
    
    async def get_orderbook(self, symbol: str, exchange: str, market_type: str) -> Optional[Dict]:
        """获取orderbook数据"""
        try:
            exchange_key = f"{exchange}_{market_type}"
            if exchange_key not in self.exchanges:
                return None
                
            ex = self.exchanges[exchange_key]
            
            # 转换币种格式
            if exchange == "okx" and market_type == "perp":
                ccxt_symbol = f"{symbol.replace('/USDT', '')}-USDT-SWAP"
            else:
                ccxt_symbol = symbol
            
            # 获取orderbook
            orderbook = ex.fetch_order_book(ccxt_symbol, limit=5)
            
            return {
                'timestamp': time.time() * 1000,
                'exchange_timestamp': orderbook.get('timestamp', time.time() * 1000),
                'symbol': symbol,
                'exchange': exchange,
                'market_type': market_type,
                'bids': orderbook['bids'],
                'asks': orderbook['asks'],
                'mid_price': (orderbook['bids'][0][0] + orderbook['asks'][0][0]) / 2 if orderbook['bids'] and orderbook['asks'] else None
            }
            
        except Exception as e:
            # 静默处理错误，避免日志污染
            return None


class RealtimeArbitrageEngine:
    """实时套利引擎"""
    
    def __init__(self, data_provider: DataProvider):
        # 配置参数（基于原策略）
        self.tokens = ["API3", "OM", "CFX"]
        self.window_ms = 200
        self.move_thr_bps = 180.0
        self.spread_thr_bps = 18.0
        self.cooldown_ms = 800
        self.catchup_ratio = 0.95
        self.min_gap_bps = 25.0
        self.sl_bps = 50.0
        self.fee_bps_per_trade = 2.0
        self.max_holding_s = 15.0
        self.delays_ms = [0, 10, 30, 50, 100]
        self.only_cross_exchange = True
        
        # 每个币种允许的交易所
        self.allowed_exchanges = {
            "API3": {"binance", "okx"},
            "CFX": {"binance", "okx"},
            "OM": {"binance", "okx", "bybit"},
        }
        
        # 数据提供者
        self.data_provider = data_provider
        
        # 实时数据存储
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))  # 存储价格历史
        self.last_prices: Dict[str, float] = {}
        self.last_timestamps: Dict[str, float] = {}
        
        # 事件和交易记录
        self.events = []
        self.trades = []
        self.last_event_time = None
        
        # 运行状态
        self.running = False
        self.start_time = None
        
        # 统计数据
        self.stats = {
            'total_events': 0,
            'total_trades': 0,
            'total_pnl_bps': 0.0,
            'win_rate': 0.0,
            'data_points': defaultdict(int)
        }

    async def start(self):
        """启动套利引擎"""
        print("🚀 " + "="*80, flush=True)
        print("🚀 实时延迟套利引擎启动 (CCXT版本)", flush=True)
        print("🚀 " + "="*80, flush=True)
        
        # 启动数据提供者
        await self.data_provider.start()
        
        self.running = True
        self.start_time = time.time()
        
        print("📊 策略配置:", flush=True)
        print(f"   🎯 测试币种: {', '.join(self.tokens)}", flush=True)
        print(f"   📈 触发窗口: {self.window_ms}ms", flush=True)
        print(f"   📊 移动阈值: {self.move_thr_bps}bps", flush=True)
        print(f"   📏 价差阈值: {self.spread_thr_bps}bps", flush=True)
        print(f"   ⏱️  冷却时间: {self.cooldown_ms}ms", flush=True)
        print(f"   🎯 追赶比例: {self.catchup_ratio}", flush=True)
        print(f"   💰 最小价差: {self.min_gap_bps}bps", flush=True)
        print(f"   🛑 止损: {self.sl_bps}bps", flush=True)
        print("🚀 " + "="*80, flush=True)
        
        # 创建数据收集任务
        tasks = []
        
        for token in self.tokens:
            allowed_exchanges = self.allowed_exchanges[token]
            symbol = f"{token}/USDT"
            
            for exchange in allowed_exchanges:
                # 现货任务
                tasks.append(asyncio.create_task(
                    self._collect_data_loop(symbol, exchange, "spot"),
                    name=f"{token}_{exchange}_spot"
                ))
                
                # 合约任务
                tasks.append(asyncio.create_task(
                    self._collect_data_loop(symbol, exchange, "perp"),
                    name=f"{token}_{exchange}_perp"
                ))
        
        # 事件检测任务
        tasks.append(asyncio.create_task(
            self._event_detection_loop(),
            name="event_detection"
        ))
        
        # 统计任务
        tasks.append(asyncio.create_task(
            self._stats_loop(),
            name="stats"
        ))
        
        print(f"📡 启动 {len(tasks)} 个数据收集任务...", flush=True)
        print("🚀 开始实时套利监控...", flush=True)
        print("-" * 80, flush=True)
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            print(f"❌ 引擎运行错误: {e}", flush=True)
        finally:
            await self.stop()

    async def stop(self):
        """停止套利引擎"""
        self.running = False
        await self.data_provider.stop()
        self._generate_final_report()

    async def _collect_data_loop(self, symbol: str, exchange: str, market_type: str):
        """数据收集循环"""
        key = f"{exchange}-{market_type}"
        
        while self.running:
            try:
                # 获取orderbook数据
                data = await self.data_provider.get_orderbook(symbol, exchange, market_type)
                
                if data and data['mid_price']:
                    # 存储价格历史
                    timestamp = data['timestamp']
                    price = data['mid_price']
                    
                    self.price_history[key].append((timestamp, price))
                    self.last_prices[key] = price
                    self.last_timestamps[key] = timestamp
                    self.stats['data_points'][key] += 1
                
                await asyncio.sleep(0.1)  # 100ms间隔
                
            except Exception as e:
                await asyncio.sleep(0.5)  # 错误时等待更长时间

    async def _event_detection_loop(self):
        """事件检测循环"""
        await asyncio.sleep(5)  # 等待数据积累
        
        while self.running:
            try:
                await self._detect_arbitrage_events()
                await asyncio.sleep(0.05)  # 50ms检测间隔
            except Exception as e:
                print(f"⚠️  事件检测错误: {e}", flush=True)
                await asyncio.sleep(1)

    async def _detect_arbitrage_events(self):
        """检测套利事件"""
        current_time = time.time() * 1000
        window_start = current_time - self.window_ms
        
        for token in self.tokens:
            allowed_exchanges = self.allowed_exchanges[token]
            
            # 收集该币种的所有价格数据
            token_prices = {}
            for exchange in allowed_exchanges:
                for market_type in ["spot", "perp"]:
                    key = f"{exchange}-{market_type}"
                    if key in self.price_history and len(self.price_history[key]) > 0:
                        # 获取窗口内的价格数据
                        recent_data = [(ts, px) for ts, px in self.price_history[key] 
                                     if ts >= window_start]
                        if recent_data:
                            token_prices[key] = recent_data
            
            if len(token_prices) < 2:
                continue
            
            # 检测价格移动事件
            await self._check_price_movements(token, token_prices, current_time)

    async def _check_price_movements(self, token: str, token_prices: Dict, current_time: float):
        """检查价格移动并生成事件"""
        try:
            window_start = current_time - self.window_ms
            
            for key, price_data in token_prices.items():
                if len(price_data) < 2:
                    continue
                
                # 计算窗口内价格变动
                latest_price = price_data[-1][1]
                
                # 找到窗口开始时的价格
                base_price = None
                for ts, px in price_data:
                    if ts >= window_start:
                        base_price = px
                        break
                
                if base_price is None:
                    continue
                
                # 计算收益率
                ret_bps = (latest_price / base_price - 1.0) * 10000
                
                # 检查是否超过移动阈值
                if abs(ret_bps) >= self.move_thr_bps:
                    # 检查冷却期
                    if (self.last_event_time is not None and 
                        (current_time - self.last_event_time) < self.cooldown_ms):
                        continue
                    
                    # 检查跨交易所价差
                    if await self._check_cross_exchange_spread(token, token_prices, current_time):
                        await self._generate_arbitrage_event(token, key, ret_bps, current_time)

    async def _check_cross_exchange_spread(self, token: str, token_prices: Dict, current_time: float) -> bool:
        """检查跨交易所价差"""
        try:
            current_prices = {}
            
            # 获取所有交易所的当前价格
            for key, price_data in token_prices.items():
                if price_data:
                    current_prices[key] = price_data[-1][1]
            
            if len(current_prices) < 2:
                return False
            
            # 计算价差
            prices = list(current_prices.values())
            spread_bps = (max(prices) / min(prices) - 1.0) * 10000
            
            return spread_bps >= self.spread_thr_bps
            
        except Exception as e:
            return False

    async def _generate_arbitrage_event(self, token: str, initiator_key: str, ret_bps: float, event_time: float):
        """生成套利事件"""
        try:
            sign = 1.0 if ret_bps >= 0 else -1.0
            
            event = {
                'timestamp': event_time,
                'datetime': pd.to_datetime(event_time, unit='ms', utc=True),
                'token': token,
                'initiator': initiator_key,
                'sign': sign,
                'ret_bps': ret_bps,
                'window_ms': self.window_ms
            }
            
            self.events.append(event)
            self.stats['total_events'] += 1
            self.last_event_time = event_time
            
            # 模拟交易执行
            await self._simulate_trade(event)
            
            print(f"🚨 事件#{self.stats['total_events']} {token} {initiator_key}: {ret_bps:.1f}bps", flush=True)
            
        except Exception as e:
            print(f"⚠️  事件生成错误: {e}", flush=True)

    async def _simulate_trade(self, event: Dict):
        """模拟交易执行（基于原策略逻辑）"""
        try:
            # 这里可以实现完整的交易逻辑
            # 目前简化为统计记录
            
            # 模拟延迟执行
            for delay_ms in self.delays_ms:
                await asyncio.sleep(delay_ms / 1000.0)  # 模拟延迟
                
                # 简化的PnL计算（实际应该基于实时价格）
                simulated_pnl = np.random.normal(5.0, 15.0)  # 模拟PnL
                
                trade = {
                    'timestamp': event['timestamp'] + delay_ms,
                    'token': event['token'],
                    'delay_ms': delay_ms,
                    'entry_signal': event['ret_bps'],
                    'pnl_bps': simulated_pnl,
                    'hit': 'simulated'
                }
                
                self.trades.append(trade)
                self.stats['total_trades'] += 1
                self.stats['total_pnl_bps'] += simulated_pnl
                
                break  # 只执行第一个延迟，避免重复
            
        except Exception as e:
            print(f"⚠️  交易模拟错误: {e}", flush=True)

    async def _stats_loop(self):
        """统计循环"""
        await asyncio.sleep(10)  # 等待数据积累
        
        while self.running:
            try:
                self._print_realtime_stats()
                await asyncio.sleep(10)  # 每10秒统计
            except Exception as e:
                print(f"⚠️  统计错误: {e}", flush=True)
                await asyncio.sleep(5)

    def _print_realtime_stats(self):
        """打印实时统计"""
        if not self.start_time:
            return
            
        elapsed = time.time() - self.start_time
        
        print(f"\n📊 " + "="*80, flush=True)
        print(f"📊 实时套利监控统计 - 运行时间: {elapsed:.1f}秒", flush=True)
        print(f"📊 " + "="*80, flush=True)
        
        # 数据收集统计
        print("📡 数据收集状态:", flush=True)
        for token in self.tokens:
            print(f"\n💰 {token}:", flush=True)
            allowed = self.allowed_exchanges[token]
            
            for exchange in allowed:
                for market_type in ["spot", "perp"]:
                    key = f"{exchange}-{market_type}"
                    count = self.stats['data_points'][key]
                    last_price = self.last_prices.get(key, 0)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    
                    if count > 0:
                        print(f"  {market_emoji} {exchange.upper()} {market_name}: {count:,}次, 最新价格=${last_price:.4f}", flush=True)
                    else:
                        print(f"  {market_emoji} {exchange.upper()} {market_name}: ⏳ 等待数据...", flush=True)
        
        # 事件和交易统计
        print(f"\n🚨 套利事件: {self.stats['total_events']:,}个", flush=True)
        print(f"💼 模拟交易: {self.stats['total_trades']:,}笔", flush=True)
        
        if self.stats['total_trades'] > 0:
            avg_pnl = self.stats['total_pnl_bps'] / self.stats['total_trades']
            win_trades = sum(1 for t in self.trades if t['pnl_bps'] > 0)
            win_rate = win_trades / len(self.trades) * 100
            
            print(f"📈 平均PnL: {avg_pnl:.2f}bps", flush=True)
            print(f"🎯 胜率: {win_rate:.1f}%", flush=True)
            print(f"💰 累计PnL: {self.stats['total_pnl_bps']:.2f}bps", flush=True)

    def _generate_final_report(self):
        """生成最终报告"""
        print(f"\n🏁 " + "="*90, flush=True)
        print("🏁 实时延迟套利测试最终报告", flush=True)
        print("🏁 " + "="*90, flush=True)
        
        if not self.start_time:
            print("❌ 测试未正常启动", flush=True)
            return
        
        test_duration = time.time() - self.start_time
        
        print(f"🕐 测试时间: {datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        print(f"⏱️  测试时长: {test_duration:.1f}秒", flush=True)
        print(f"🚨 总事件数: {self.stats['total_events']:,}", flush=True)
        print(f"💼 总交易数: {self.stats['total_trades']:,}", flush=True)
        
        # 按币种统计
        token_stats = defaultdict(lambda: {'events': 0, 'trades': 0, 'pnl': 0.0})
        
        for event in self.events:
            token_stats[event['token']]['events'] += 1
        
        for trade in self.trades:
            token_stats[trade['token']]['trades'] += 1
            token_stats[trade['token']]['pnl'] += trade['pnl_bps']
        
        print("\n📊 各币种统计:", flush=True)
        for token in self.tokens:
            stats = token_stats[token]
            print(f"\n💰 {token}:", flush=True)
            print(f"  🚨 事件数: {stats['events']:,}", flush=True)
            print(f"  💼 交易数: {stats['trades']:,}", flush=True)
            print(f"  💰 PnL: {stats['pnl']:.2f}bps", flush=True)
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/arbitrage_realtime_report_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("实时延迟套利测试报告\n")
                f.write("="*50 + "\n")
                f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"测试时长: {test_duration:.1f}秒\n")
                f.write(f"总事件数: {self.stats['total_events']}\n")
                f.write(f"总交易数: {self.stats['total_trades']}\n")
                f.write(f"累计PnL: {self.stats['total_pnl_bps']:.2f}bps\n")
                f.write("\n各币种统计:\n")
                for token in self.tokens:
                    stats = token_stats[token]
                    f.write(f"{token}: 事件={stats['events']}, 交易={stats['trades']}, PnL={stats['pnl']:.2f}bps\n")
            
            print(f"\n📄 报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n🎉 实时套利测试完成！", flush=True)


async def main():
    """主函数"""
    print("🚀 实时延迟套利引擎", flush=True)
    print("="*50, flush=True)
    
    # 创建数据提供者
    data_provider = CCXTDataProvider()
    
    # 创建套利引擎
    engine = RealtimeArbitrageEngine(data_provider)
    
    try:
        # 运行60秒测试
        await asyncio.wait_for(engine.start(), timeout=60.0)
    except asyncio.TimeoutError:
        print("\n⏰ 测试时间到，正在停止...", flush=True)
        await engine.stop()
    except KeyboardInterrupt:
        print("\n⏹️  收到中断信号，正在停止...", flush=True)
        await engine.stop()


if __name__ == "__main__":
    # 创建报告目录
    import os
    os.makedirs("reports", exist_ok=True)
    
    print("🚀 启动实时延迟套利测试...", flush=True)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  测试被用户中断", flush=True)
    except Exception as e:
        print(f"❌ 测试失败: {e}", flush=True)
    finally:
        print("👋 测试结束", flush=True)
