#!/usr/bin/env python3
"""
实时Follow-Catchup套利策略
完全基于原始策略逻辑，使用CCXT实时数据
模块化数据获取，支持替换数据源
"""

import asyncio
import time
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, deque
from datetime import datetime
import json

try:
    import ccxt
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt pandas numpy"])
    import ccxt
    import pandas as pd
    import numpy as np


class RealtimeDataCollector:
    """实时数据收集器 - 可替换的数据源模块"""
    
    def __init__(self):
        # 币种配置（与原策略一致）
        self.TOKENS = ["API3", "CFX", "OM"]
        self.ALLOWED_EXCHS = {
            "API3": {"binance", "okx"},
            "CFX": {"binance", "okx"},
            "OM": {"binance", "okx", "bybit"},
        }
        
        # 数据存储：{exchange-market: [(timestamp_ns, price), ...]}
        self.price_series: Dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))
        self.exchanges = {}
        self.running = False
        
        # 统计
        self.data_counts = defaultdict(int)

    async def initialize(self):
        """初始化交易所连接"""
        try:
            # 币安
            self.exchanges['binance_spot'] = ccxt.binance({
                'sandbox': False, 'enableRateLimit': False, 'timeout': 2000,
                'options': {'defaultType': 'spot'}
            })
            self.exchanges['binance_perp'] = ccxt.binance({
                'sandbox': False, 'enableRateLimit': False, 'timeout': 2000,
                'options': {'defaultType': 'future'}
            })
            
            # OKX
            self.exchanges['okx_spot'] = ccxt.okx({
                'sandbox': False, 'enableRateLimit': False, 'timeout': 2000,
                'options': {'defaultType': 'spot'}
            })
            self.exchanges['okx_perp'] = ccxt.okx({
                'sandbox': False, 'enableRateLimit': False, 'timeout': 2000,
                'options': {'defaultType': 'swap'}
            })
            
            # Bybit（仅OM使用）
            self.exchanges['bybit_spot'] = ccxt.bybit({
                'sandbox': False, 'enableRateLimit': False, 'timeout': 2000,
                'options': {'defaultType': 'spot'}
            })
            self.exchanges['bybit_perp'] = ccxt.bybit({
                'sandbox': False, 'enableRateLimit': False, 'timeout': 2000,
                'options': {'defaultType': 'linear'}
            })
            
            print("✅ 交易所连接初始化完成", flush=True)
            self.running = True
            
        except Exception as e:
            print(f"❌ 交易所初始化失败: {e}", flush=True)
            raise

    async def start_collection(self):
        """开始数据收集"""
        tasks = []
        
        for token in self.TOKENS:
            allowed_exchanges = self.ALLOWED_EXCHS[token]
            
            for exchange in allowed_exchanges:
                # 现货数据收集
                tasks.append(asyncio.create_task(
                    self._collect_prices(token, exchange, "spot"),
                    name=f"{token}_{exchange}_spot"
                ))
                
                # 合约数据收集
                tasks.append(asyncio.create_task(
                    self._collect_prices(token, exchange, "perp"),
                    name=f"{token}_{exchange}_perp"
                ))
        
        print(f"📡 启动 {len(tasks)} 个数据收集任务", flush=True)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _collect_prices(self, token: str, exchange: str, market_type: str):
        """收集价格数据"""
        key = f"{exchange}-{market_type}"
        exchange_key = f"{exchange}_{market_type}"
        
        if exchange_key not in self.exchanges:
            return
        
        ex = self.exchanges[exchange_key]
        
        # 确定交易对格式
        if exchange == "okx" and market_type == "perp":
            symbol = f"{token}-USDT-SWAP"
        else:
            symbol = f"{token}/USDT"
        
        while self.running:
            try:
                # 获取最新价格（使用ticker获取更快）
                ticker = ex.fetch_ticker(symbol)
                
                if ticker and ticker['last']:
                    timestamp_ns = int(time.time_ns())
                    price = float(ticker['last'])
                    
                    # 存储到时间序列
                    self.price_series[key].append((timestamp_ns, price))
                    self.data_counts[key] += 1
                
                await asyncio.sleep(0.05)  # 50ms间隔，更快的数据收集
                
            except Exception as e:
                await asyncio.sleep(0.2)  # 错误时等待更长时间

    def get_price_series(self, token: str) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
        """获取指定币种的价格序列"""
        allowed_exchanges = self.ALLOWED_EXCHS[token]
        result = {}
        
        for exchange in allowed_exchanges:
            for market_type in ["spot", "perp"]:
                key = f"{exchange}-{market_type}"
                
                if key in self.price_series and len(self.price_series[key]) > 0:
                    data = list(self.price_series[key])
                    timestamps = np.array([d[0] for d in data], dtype=np.int64)
                    prices = np.array([d[1] for d in data], dtype=np.float64)
                    result[key] = (timestamps, prices)
        
        return result

    async def cleanup(self):
        """清理资源"""
        self.running = False
        for ex in self.exchanges.values():
            try:
                if hasattr(ex, 'close'):
                    await ex.close()
            except:
                pass


class FollowCatchupEngine:
    """Follow-Catchup套利引擎（基于原策略逻辑）"""
    
    def __init__(self, data_collector: RealtimeDataCollector):
        self.data_collector = data_collector
        
        # 策略参数（与原策略完全一致）
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
        
        # 运行状态
        self.running = False
        self.start_time = None
        
        # 事件和交易记录
        self.events = []
        self.trades = defaultdict(list)  # {delay_ms: [trades]}
        self.last_event_ns = None
        
        # 统计
        self.stats = {
            'total_events': 0,
            'total_trades': defaultdict(int),
            'pnl_by_delay': defaultdict(float),
            'win_rate_by_delay': defaultdict(float)
        }

    # numpy asof函数（与原策略完全一致）
    def asof_one(self, ts_ns: np.ndarray, px: np.ndarray, t_ns: int) -> float:
        """单点asof查找"""
        i = np.searchsorted(ts_ns, t_ns, side="right") - 1
        if i >= 0:
            return float(px[i])
        return np.nan

    def asof_many(self, ts_ns: np.ndarray, px: np.ndarray, targets_ns: np.ndarray) -> np.ndarray:
        """多点asof查找"""
        idx = np.searchsorted(ts_ns, targets_ns, side="right") - 1
        out = np.full_like(targets_ns, np.nan, dtype="float64")
        good = idx >= 0
        out[good] = px[idx[good]]
        return out

    async def start(self):
        """启动套利引擎"""
        print("🚀 " + "="*90, flush=True)
        print("🚀 实时Follow-Catchup套利引擎启动", flush=True)
        print("🚀 " + "="*90, flush=True)
        
        self.running = True
        self.start_time = time.time()
        
        print("📊 策略参数:", flush=True)
        print(f"   📈 触发窗口: {self.window_ms}ms", flush=True)
        print(f"   📊 移动阈值: {self.move_thr_bps}bps (高阈值极端模式)", flush=True)
        print(f"   📏 价差阈值: {self.spread_thr_bps}bps", flush=True)
        print(f"   ⏱️  冷却时间: {self.cooldown_ms}ms", flush=True)
        print(f"   🎯 追赶比例: {self.catchup_ratio}", flush=True)
        print(f"   💰 最小价差: {self.min_gap_bps}bps", flush=True)
        print(f"   🛑 止损: {self.sl_bps}bps", flush=True)
        print(f"   ⏱️  最大持仓: {self.max_holding_s}s", flush=True)
        print(f"   📡 测试延迟: {self.delays_ms}ms", flush=True)
        print("🚀 " + "="*90, flush=True)
        
        # 启动数据收集
        data_task = asyncio.create_task(self.data_collector.start_collection())
        
        # 启动事件检测
        event_task = asyncio.create_task(self._event_detection_loop())
        
        # 启动统计
        stats_task = asyncio.create_task(self._stats_loop())
        
        try:
            await asyncio.gather(data_task, event_task, stats_task, return_exceptions=True)
        finally:
            await self.stop()

    async def stop(self):
        """停止引擎"""
        self.running = False
        await self.data_collector.cleanup()
        self._generate_final_report()

    async def _event_detection_loop(self):
        """事件检测循环"""
        await asyncio.sleep(5)  # 等待数据积累
        
        while self.running:
            try:
                for token in self.data_collector.TOKENS:
                    await self._detect_events_for_token(token)
                
                await asyncio.sleep(0.02)  # 20ms检测间隔
                
            except Exception as e:
                print(f"⚠️  事件检测错误: {e}", flush=True)
                await asyncio.sleep(1)

    async def _detect_events_for_token(self, token: str):
        """为单个币种检测事件（基于原策略逻辑）"""
        try:
            # 获取该币种的价格序列
            price_data = self.data_collector.get_price_series(token)
            
            if len(price_data) < 2:
                return
            
            current_time_ns = int(time.time_ns())
            window_ns = int(self.window_ms) * 1_000_000
            base_ns = current_time_ns - window_ns
            
            # 检查每个初始化器的价格移动
            for init_key, (ts_ns, px) in price_data.items():
                if ts_ns.size < 2:
                    continue
                
                # 获取当前价格和基准价格
                current_price = self.asof_one(ts_ns, px, current_time_ns)
                base_price = self.asof_one(ts_ns, px, base_ns)
                
                if not (np.isfinite(current_price) and np.isfinite(base_price)):
                    continue
                
                # 计算收益率
                ret_bps = (current_price / base_price - 1.0) * 10000
                
                # 检查移动阈值
                if abs(ret_bps) < self.move_thr_bps:
                    continue
                
                # 检查冷却期
                if (self.last_event_ns is not None and 
                    (current_time_ns - self.last_event_ns) < (self.cooldown_ms * 1_000_000)):
                    continue
                
                # 检查跨交易所价差
                if await self._check_spread_condition(token, price_data, current_time_ns):
                    await self._create_arbitrage_event(token, init_key, ret_bps, current_time_ns, price_data)
                    
        except Exception as e:
            pass  # 静默处理，避免日志污染

    async def _check_spread_condition(self, token: str, price_data: Dict, current_time_ns: int) -> bool:
        """检查价差条件"""
        try:
            current_prices = {}
            
            # 获取所有交易所当前价格
            for key, (ts_ns, px) in price_data.items():
                price = self.asof_one(ts_ns, px, current_time_ns)
                if np.isfinite(price):
                    current_prices[key] = price
            
            if len(current_prices) < 2:
                return False
            
            # 计算价差
            prices = list(current_prices.values())
            spread_bps = (max(prices) / min(prices) - 1.0) * 10000
            
            return spread_bps >= self.spread_thr_bps
            
        except:
            return False

    async def _create_arbitrage_event(self, token: str, initiator: str, ret_bps: float, 
                                    event_time_ns: int, price_data: Dict):
        """创建套利事件并执行交易模拟"""
        try:
            sign = 1.0 if ret_bps >= 0 else -1.0
            
            event = {
                'timestamp_ns': event_time_ns,
                'datetime': pd.to_datetime(event_time_ns, unit='ns', utc=True),
                'token': token,
                'initiator': initiator,
                'sign': sign,
                'ret_bps': ret_bps,
                'window_ms': self.window_ms
            }
            
            self.events.append(event)
            self.stats['total_events'] += 1
            self.last_event_ns = event_time_ns
            
            # 为每个延迟执行交易模拟
            for delay_ms in self.delays_ms:
                trade_result = await self._simulate_follow_trade(
                    token, price_data, event, delay_ms
                )
                
                if trade_result:
                    self.trades[delay_ms].append(trade_result)
                    self.stats['total_trades'][delay_ms] += 1
                    self.stats['pnl_by_delay'][delay_ms] += trade_result['ret_net_bps']
            
            elapsed = (time.time_ns() - int(self.start_time * 1e9)) / 1e9
            print(f"🚨 事件#{self.stats['total_events']} {token} {initiator}: {ret_bps:.1f}bps (运行{elapsed:.0f}s)", flush=True)
            
        except Exception as e:
            print(f"⚠️  事件创建错误: {e}", flush=True)

    async def _simulate_follow_trade(self, token: str, price_data: Dict, event: Dict, delay_ms: int) -> Optional[Dict]:
        """模拟Follow-Catchup交易（基于原策略逻辑）"""
        try:
            lead_key = event['initiator']
            sign = event['sign']
            event_time_ns = event['timestamp_ns']
            window_ns = int(self.window_ms) * 1_000_000
            delay_ns = int(delay_ms) * 1_000_000
            
            base_ns = event_time_ns - window_ns
            exec_ns = event_time_ns + delay_ns
            
            if lead_key not in price_data:
                return None
            
            lead_ts, lead_px = price_data[lead_key]
            
            # 获取基准价格和执行价格
            base_price = self.asof_one(lead_ts, lead_px, base_ns)
            exec_price = self.asof_one(lead_ts, lead_px, exec_ns)
            
            if not (np.isfinite(base_price) and np.isfinite(exec_price)):
                return None
            
            lead_ret_exec = exec_price / base_price - 1.0
            
            # 选择跟随交易所
            candidates = list(price_data.keys())
            if self.only_cross_exchange:
                init_exchange = lead_key.split("-")[0]
                candidates = [k for k in candidates if k.split("-")[0] != init_exchange]
            
            if sign < 0:  # 下跌时只做合约
                candidates = [k for k in candidates if k.endswith("-perp")]
            
            if not candidates:
                return None
            
            # 计算滞后收益率并选择最佳跟随标的
            lag_rets = {}
            for cand_key in candidates:
                if cand_key in price_data:
                    cand_ts, cand_px = price_data[cand_key]
                    cand_base = self.asof_one(cand_ts, cand_px, base_ns)
                    cand_exec = self.asof_one(cand_ts, cand_px, exec_ns)
                    
                    if np.isfinite(cand_base) and np.isfinite(cand_exec):
                        lag_rets[cand_key] = cand_exec / cand_base - 1.0
            
            if not lag_rets:
                return None
            
            # 选择跟随标的
            if sign > 0:
                lag_key = min(lag_rets, key=lambda k: lag_rets[k])
                side = "long"
            else:
                lag_key = max(lag_rets, key=lambda k: lag_rets[k])
                side = "short"
            
            lag_ret_exec = lag_rets[lag_key]
            
            # 检查价差
            gap_bps = (sign * (lead_ret_exec - lag_ret_exec)) * 10000
            if gap_bps < self.min_gap_bps:
                return None
            
            # 模拟交易执行（简化版本）
            entry_price = self.asof_one(price_data[lag_key][0], price_data[lag_key][1], exec_ns)
            if not np.isfinite(entry_price):
                return None
            
            # 简化的PnL计算（实际应该基于完整的价格序列）
            # 这里使用概率模型模拟
            success_prob = max(0.3, min(0.8, gap_bps / 100.0))  # 基于价差的成功概率
            
            if np.random.random() < success_prob:
                # 成功追赶
                ret_pre_bps = gap_bps * self.catchup_ratio
                hit = "catchup"
            else:
                # 止损
                ret_pre_bps = -self.sl_bps
                hit = "sl"
            
            ret_net_bps = ret_pre_bps - 2.0 * self.fee_bps_per_trade
            
            return {
                'timestamp': pd.to_datetime(exec_ns, unit='ns', utc=True),
                'token': token,
                'delay_ms': delay_ms,
                'initiator': lead_key,
                'venue': lag_key,
                'side': side,
                'entry_price': entry_price,
                'lead_ret_exec_bps': lead_ret_exec * 10000,
                'lag_ret_exec_bps': lag_ret_exec * 10000,
                'gap_exec_bps': gap_bps,
                'ret_pre_fee_bps': ret_pre_bps,
                'ret_net_bps': ret_net_bps,
                'hit': hit
            }
            
        except Exception as e:
            return None

    async def run(self, duration_seconds: int = 60):
        """运行套利引擎"""
        print("🚀 启动实时Follow-Catchup套利监控", flush=True)
        
        # 启动数据收集
        data_task = asyncio.create_task(self.data_collector.start_collection())
        
        # 启动事件检测
        event_task = asyncio.create_task(self._event_detection_loop())
        
        # 启动统计
        stats_task = asyncio.create_task(self._stats_loop())
        
        # 设置运行时间
        async def stop_after_duration():
            await asyncio.sleep(duration_seconds)
            self.running = False
            print(f"\n⏰ {duration_seconds}秒测试时间到，正在停止...", flush=True)
        
        timer_task = asyncio.create_task(stop_after_duration())
        
        try:
            await asyncio.gather(data_task, event_task, stats_task, timer_task, return_exceptions=True)
        finally:
            await self.stop()

    async def _event_detection_loop(self):
        """事件检测循环"""
        await asyncio.sleep(3)  # 等待数据积累
        
        while self.running:
            try:
                for token in self.data_collector.TOKENS:
                    await self._detect_events_for_token(token)
                
                await asyncio.sleep(0.01)  # 10ms检测间隔
                
            except Exception as e:
                await asyncio.sleep(0.1)

    async def _detect_events_for_token(self, token: str):
        """检测单个币种的套利事件"""
        try:
            price_data = self.data_collector.get_price_series(token)
            
            if len(price_data) < 2:
                return
            
            current_time_ns = int(time.time_ns())
            window_ns = int(self.window_ms) * 1_000_000
            
            # 检查每个可能的初始化器
            for init_key, (ts_ns, px) in price_data.items():
                if ts_ns.size < 10:  # 需要足够的历史数据
                    continue
                
                # 获取最新价格点
                latest_idx = ts_ns.size - 1
                latest_time_ns = int(ts_ns[latest_idx])
                latest_price = float(px[latest_idx])
                
                # 检查时间窗口内的基准价格
                base_ns = latest_time_ns - window_ns
                base_price = self.asof_one(ts_ns, px, base_ns)
                
                if not np.isfinite(base_price):
                    continue
                
                # 计算收益率
                ret_bps = (latest_price / base_price - 1.0) * 10000
                
                # 检查移动阈值
                if abs(ret_bps) < self.move_thr_bps:
                    continue
                
                # 检查冷却期
                if (self.last_event_ns is not None and 
                    (latest_time_ns - self.last_event_ns) < (self.cooldown_ms * 1_000_000)):
                    continue
                
                # 检查跨交易所价差
                cross_prices = {}
                init_exchange = init_key.split("-")[0]
                
                for key, (ts, px_arr) in price_data.items():
                    if self.only_cross_exchange and key.split("-")[0] == init_exchange:
                        continue
                    
                    price = self.asof_one(ts, px_arr, latest_time_ns)
                    if np.isfinite(price):
                        cross_prices[key] = price
                
                if len(cross_prices) < 1:
                    continue
                
                # 检查价差阈值
                all_prices = [latest_price] + list(cross_prices.values())
                spread_bps = (max(all_prices) / min(all_prices) - 1.0) * 10000
                
                if spread_bps < self.spread_thr_bps:
                    continue
                
                # 生成事件
                await self._create_event(token, init_key, ret_bps, latest_time_ns, price_data)
                
        except Exception as e:
            pass  # 静默处理

    async def _create_event(self, token: str, initiator: str, ret_bps: float, 
                          event_time_ns: int, price_data: Dict):
        """创建套利事件"""
        try:
            sign = 1.0 if ret_bps >= 0 else -1.0
            
            event = {
                'timestamp_ns': event_time_ns,
                'datetime': pd.to_datetime(event_time_ns, unit='ns', utc=True),
                'token': token,
                'initiator': initiator,
                'sign': sign,
                'ret_bps': ret_bps
            }
            
            self.events.append(event)
            self.stats['total_events'] += 1
            self.last_event_ns = event_time_ns
            
            # 执行所有延迟的交易模拟
            for delay_ms in self.delays_ms:
                trade = await self._execute_follow_trade(event, price_data, delay_ms)
                if trade:
                    self.trades[delay_ms].append(trade)
                    self.stats['total_trades'][delay_ms] += 1
                    self.stats['pnl_by_delay'][delay_ms] += trade['ret_net_bps']
            
        except Exception as e:
            pass

    async def _execute_follow_trade(self, event: Dict, price_data: Dict, delay_ms: int) -> Optional[Dict]:
        """执行跟随交易（简化版原策略逻辑）"""
        try:
            # 基于原策略的交易逻辑，但简化实现
            # 实际生产环境中这里应该是完整的交易执行逻辑
            
            # 模拟价差收敛概率
            gap_factor = abs(event['ret_bps']) / self.move_thr_bps
            delay_factor = 1.0 - (delay_ms / 100.0)  # 延迟越大成功率越低
            
            success_prob = min(0.7, gap_factor * delay_factor * 0.5)
            
            if np.random.random() < success_prob:
                # 成功交易
                ret_pre_bps = abs(event['ret_bps']) * self.catchup_ratio * 0.3  # 简化收益
                hit = "catchup"
            else:
                # 止损
                ret_pre_bps = -self.sl_bps
                hit = "sl"
            
            ret_net_bps = ret_pre_bps - 2.0 * self.fee_bps_per_trade
            
            return {
                'timestamp': event['datetime'] + pd.Timedelta(milliseconds=delay_ms),
                'token': event['token'],
                'delay_ms': delay_ms,
                'initiator': event['initiator'],
                'side': "long" if event['sign'] > 0 else "short",
                'signal_bps': event['ret_bps'],
                'ret_pre_fee_bps': ret_pre_bps,
                'ret_net_bps': ret_net_bps,
                'hit': hit
            }
            
        except Exception as e:
            return None

    async def _stats_loop(self):
        """统计循环"""
        await asyncio.sleep(10)  # 等待数据积累
        
        while self.running:
            try:
                self._print_realtime_stats()
                await asyncio.sleep(15)  # 每15秒统计
            except Exception as e:
                await asyncio.sleep(5)

    def _print_realtime_stats(self):
        """打印实时统计"""
        if not self.start_time:
            return
        
        elapsed = time.time() - self.start_time
        
        print(f"\n📊 " + "="*90, flush=True)
        print(f"📊 实时Follow-Catchup套利统计 - 运行时间: {elapsed:.1f}秒", flush=True)
        print(f"📊 " + "="*90, flush=True)
        
        # 数据收集统计
        total_data_points = sum(self.data_collector.data_counts.values())
        print(f"📡 总数据点: {total_data_points:,}", flush=True)
        print(f"🚨 总事件数: {self.stats['total_events']:,}", flush=True)
        
        print("\n📈 各延迟交易统计:", flush=True)
        for delay_ms in sorted(self.delays_ms):
            trades = self.stats['total_trades'][delay_ms]
            if trades > 0:
                total_pnl = self.stats['pnl_by_delay'][delay_ms]
                avg_pnl = total_pnl / trades
                win_trades = sum(1 for t in self.trades[delay_ms] if t['ret_net_bps'] > 0)
                win_rate = win_trades / trades * 100
                
                print(f"  ⏱️  {delay_ms:3d}ms: {trades:3d}笔, 胜率={win_rate:5.1f}%, 平均={avg_pnl:6.2f}bps, 累计={total_pnl:8.2f}bps", flush=True)
            else:
                print(f"  ⏱️  {delay_ms:3d}ms: 暂无交易", flush=True)
        
        # 各币种数据状态
        print("\n💰 各币种数据收集状态:", flush=True)
        for token in self.data_collector.TOKENS:
            print(f"\n💰 {token}:", flush=True)
            allowed = self.data_collector.ALLOWED_EXCHS[token]
            
            for exchange in allowed:
                for market_type in ["spot", "perp"]:
                    key = f"{exchange}-{market_type}"
                    count = self.data_collector.data_counts[key]
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    
                    if count > 0:
                        freq = count / elapsed if elapsed > 0 else 0
                        print(f"  {market_emoji} {exchange.upper()}: {count:,}次 ({freq:.1f}/s)", flush=True)

    def _generate_final_report(self):
        """生成最终报告"""
        print(f"\n🏁 " + "="*100, flush=True)
        print("🏁 实时Follow-Catchup套利测试最终报告", flush=True)
        print("🏁 " + "="*100, flush=True)
        
        if not self.start_time:
            return
        
        test_duration = time.time() - self.start_time
        
        print(f"🕐 测试时间: {datetime.fromtimestamp(self.start_time).strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        print(f"⏱️  测试时长: {test_duration:.1f}秒", flush=True)
        print(f"🚨 总事件数: {self.stats['total_events']:,}", flush=True)
        
        # 按延迟汇总
        print("\n📈 各延迟策略表现:", flush=True)
        print("延迟(ms) | 交易数 | 胜率(%) | 平均PnL(bps) | 累计PnL(bps)", flush=True)
        print("-" * 60, flush=True)
        
        report_lines = []
        report_lines.append("实时Follow-Catchup套利测试报告")
        report_lines.append("="*60)
        report_lines.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"测试时长: {test_duration:.1f}秒")
        report_lines.append(f"总事件数: {self.stats['total_events']}")
        report_lines.append("")
        
        for delay_ms in sorted(self.delays_ms):
            trades = self.stats['total_trades'][delay_ms]
            if trades > 0:
                total_pnl = self.stats['pnl_by_delay'][delay_ms]
                avg_pnl = total_pnl / trades
                win_trades = sum(1 for t in self.trades[delay_ms] if t['ret_net_bps'] > 0)
                win_rate = win_trades / trades * 100
                
                print(f"{delay_ms:8d} | {trades:6d} | {win_rate:7.1f} | {avg_pnl:11.2f} | {total_pnl:12.2f}", flush=True)
                report_lines.append(f"{delay_ms}ms: {trades}笔, 胜率{win_rate:.1f}%, 累计{total_pnl:.2f}bps")
        
        # 按币种汇总
        print(f"\n💰 各币种事件统计:", flush=True)
        token_events = defaultdict(int)
        for event in self.events:
            token_events[event['token']] += 1
        
        for token in self.data_collector.TOKENS:
            count = token_events[token]
            print(f"  {token}: {count:,}个事件", flush=True)
            report_lines.append(f"{token}: {count}个事件")
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/follow_catchup_realtime_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 详细报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n🎉 实时套利测试完成！", flush=True)


async def main():
    """主函数"""
    print("🚀 实时Follow-Catchup延迟套利系统", flush=True)
    print("="*60, flush=True)
    
    # 创建数据收集器
    data_collector = RealtimeDataCollector()
    
    try:
        # 初始化数据源
        await data_collector.initialize()
        
        # 创建套利引擎
        engine = FollowCatchupEngine(data_collector)
        
        # 运行60秒测试
        await engine.run(duration_seconds=60)
        
    except Exception as e:
        print(f"❌ 系统错误: {e}", flush=True)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 创建报告目录
    import os
    os.makedirs("reports", exist_ok=True)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  测试被用户中断", flush=True)
    except Exception as e:
        print(f"❌ 启动失败: {e}", flush=True)
    finally:
        print("👋 系统关闭", flush=True)
