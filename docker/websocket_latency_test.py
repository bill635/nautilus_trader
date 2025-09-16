#!/usr/bin/env python3
"""
币安Orderbook延迟测试 - 纯WebSocket版本
不依赖NautilusTrader，直接使用币安WebSocket API
立即可用，无需复杂配置
"""

import asyncio
import json
import time
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

try:
    import websockets
except ImportError:
    print("安装websockets依赖...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets


class BinanceWebSocketLatencyTester:
    """币安WebSocket延迟测试器"""
    
    def __init__(self):
        # 测试配置
        self.symbols = ["API3USDT", "OMUSDT", "CFXUSDT"]
        self.spot_ws_url = "wss://stream.binance.com:9443/ws/"
        self.futures_ws_url = "wss://fstream.binance.com/ws/"
        
        # 数据存储
        self.latencies: Dict[str, List[float]] = defaultdict(list)
        self.message_counts: Dict[str, int] = defaultdict(int)
        
        # 测试参数
        self.test_duration = 60  # 秒
        self.running = True
        self.start_time = None
        self.total_messages = 0

    def create_subscription_message(self, symbols: List[str]) -> str:
        """创建订阅消息"""
        streams = []
        for symbol in symbols:
            # 订阅5档深度数据，100ms更新
            streams.append(f"{symbol.lower()}@depth5@100ms")
        
        subscription = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        return json.dumps(subscription)

    async def handle_websocket_messages(self, websocket, market_type: str):
        """处理WebSocket消息"""
        try:
            async for message in websocket:
                if not self.running:
                    break
                    
                try:
                    data = json.loads(message)
                    if 'stream' in data and 'data' in data:
                        await self.process_depth_message(data, market_type)
                except json.JSONDecodeError:
                    continue
                    
        except websockets.exceptions.ConnectionClosed:
            print(f"❌ {market_type} WebSocket连接断开", flush=True)
        except Exception as e:
            print(f"❌ {market_type} WebSocket错误: {e}", flush=True)

    async def process_depth_message(self, message: dict, market_type: str):
        """处理深度数据消息"""
        try:
            stream = message['stream']
            data = message['data']
            
            # 提取交易对符号
            symbol = stream.split('@')[0].upper()
            
            # 计算延迟
            receive_time = time.time() * 1000  # 毫秒
            exchange_time = data.get('E', receive_time)  # 事件时间
            latency_ms = receive_time - exchange_time
            
            # 存储数据
            key = f"{symbol}_{market_type}"
            self.latencies[key].append(latency_ms)
            self.message_counts[key] += 1
            self.total_messages += 1
            
            # 每收到20条消息显示一次进度
            if self.total_messages % 20 == 0:
                elapsed = time.time() - self.start_time if self.start_time else 0
                base_symbol = symbol.replace('USDT', '')
                market_name = "现货" if market_type == "spot" else "合约"
                print(f"📊 #{self.total_messages} {base_symbol} {market_name}: {latency_ms:.1f}ms (运行{elapsed:.0f}s)", flush=True)
            
        except Exception as e:
            print(f"⚠️  处理消息错误: {e}", flush=True)

    async def connect_spot_websocket(self):
        """连接现货WebSocket"""
        subscription = self.create_subscription_message(self.symbols)
        
        try:
            async with websockets.connect(self.spot_ws_url) as websocket:
                await websocket.send(subscription)
                print("✅ 现货WebSocket已连接并订阅", flush=True)
                await self.handle_websocket_messages(websocket, "spot")
        except Exception as e:
            print(f"❌ 现货WebSocket连接失败: {e}", flush=True)

    async def connect_futures_websocket(self):
        """连接合约WebSocket"""
        subscription = self.create_subscription_message(self.symbols)
        
        try:
            async with websockets.connect(self.futures_ws_url) as websocket:
                await websocket.send(subscription)
                print("✅ 合约WebSocket已连接并订阅", flush=True)
                await self.handle_websocket_messages(websocket, "futures")
        except Exception as e:
            print(f"❌ 合约WebSocket连接失败: {e}", flush=True)

    async def print_stats_periodically(self):
        """定期打印统计信息"""
        while self.running:
            await asyncio.sleep(5)  # 每5秒
            if self.start_time:
                self.print_current_stats()

    def print_current_stats(self):
        """打印当前统计"""
        elapsed = time.time() - self.start_time
        
        print(f"\n📈 延迟统计 - 运行时间: {elapsed:.0f}秒", flush=True)
        print(f"📊 总消息数: {self.total_messages:,}", flush=True)
        print("="*60, flush=True)
        
        for symbol in self.symbols:
            base_symbol = symbol.replace('USDT', '')
            print(f"\n💰 {base_symbol}:", flush=True)
            
            # 现货统计
            spot_key = f"{symbol}_spot"
            if spot_key in self.latencies and self.latencies[spot_key]:
                recent_latencies = self.latencies[spot_key][-30:]  # 最近30个
                avg_lat = statistics.mean(recent_latencies)
                min_lat = min(recent_latencies)
                max_lat = max(recent_latencies)
                count = self.message_counts[spot_key]
                
                print(f"  🏪 现货: 消息={count:,}, 平均={avg_lat:.2f}ms, 范围={min_lat:.2f}-{max_lat:.2f}ms", flush=True)
            else:
                print("  🏪 现货: ⏳ 等待数据...", flush=True)
            
            # 合约统计
            futures_key = f"{symbol}_futures"
            if futures_key in self.latencies and self.latencies[futures_key]:
                recent_latencies = self.latencies[futures_key][-30:]
                avg_lat = statistics.mean(recent_latencies)
                min_lat = min(recent_latencies)
                max_lat = max(recent_latencies)
                count = self.message_counts[futures_key]
                
                print(f"  📈 合约: 消息={count:,}, 平均={avg_lat:.2f}ms, 范围={min_lat:.2f}-{max_lat:.2f}ms", flush=True)
            else:
                print("  📈 合约: ⏳ 等待数据...", flush=True)

    def generate_final_report(self):
        """生成最终报告"""
        print(f"\n🏁 " + "="*70, flush=True)
        print("🏁 币安Orderbook延迟测试完成！", flush=True)
        print("🏁 " + "="*70, flush=True)
        
        if not self.start_time:
            print("❌ 测试未正常启动", flush=True)
            return
            
        total_duration = time.time() - self.start_time
        print(f"⏱️  实际测试时长: {total_duration:.1f}秒", flush=True)
        print(f"📊 总消息数量: {self.total_messages:,}", flush=True)
        
        # 详细统计
        all_latencies = []
        report_lines = []
        report_lines.append("币安Orderbook延迟测试报告 (WebSocket版本)")
        report_lines.append("="*60)
        report_lines.append(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"测试时长: {total_duration:.1f}秒")
        report_lines.append(f"总消息数: {self.total_messages:,}")
        report_lines.append("")
        
        for symbol in self.symbols:
            base_symbol = symbol.replace('USDT', '')
            print(f"\n💰 {base_symbol} 详细统计:", flush=True)
            report_lines.append(f"{base_symbol} 统计:")
            
            for market_type in ["spot", "futures"]:
                key = f"{symbol}_{market_type}"
                if key in self.latencies and self.latencies[key]:
                    latencies = self.latencies[key]
                    count = self.message_counts[key]
                    
                    avg_latency = statistics.mean(latencies)
                    min_latency = min(latencies)
                    max_latency = max(latencies)
                    median_latency = statistics.median(latencies)
                    std_latency = statistics.stdev(latencies) if len(latencies) > 1 else 0
                    
                    all_latencies.extend(latencies)
                    
                    # 延迟分布
                    fast_count = sum(1 for l in latencies if l < 10)
                    medium_count = sum(1 for l in latencies if 10 <= l < 50)
                    slow_count = sum(1 for l in latencies if l >= 50)
                    
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    symbol_display = f"{base_symbol}USDT{'-PERP' if market_type == 'futures' else ''}"
                    
                    # 控制台输出
                    print(f"  {market_emoji} {market_name} ({symbol_display}):", flush=True)
                    print(f"     📊 消息数量: {count:,}", flush=True)
                    print(f"     ⚡ 平均延迟: {avg_latency:.3f}ms", flush=True)
                    print(f"     📈 标准差: {std_latency:.3f}ms", flush=True)
                    print(f"     🎯 中位延迟: {median_latency:.3f}ms", flush=True)
                    print(f"     📏 延迟范围: {min_latency:.3f} - {max_latency:.3f}ms", flush=True)
                    print(f"     📈 分布: <10ms({fast_count/count*100:.1f}%) 10-50ms({medium_count/count*100:.1f}%) >=50ms({slow_count/count*100:.1f}%)", flush=True)
                    
                    # 报告内容
                    report_lines.append(f"  {market_name}: 消息={count:,}, 平均={avg_latency:.3f}ms, 中位={median_latency:.3f}ms")
                    report_lines.append(f"    延迟范围: {min_latency:.3f} - {max_latency:.3f}ms")
                    report_lines.append(f"    分布: <10ms({fast_count/count*100:.1f}%) 10-50ms({medium_count/count*100:.1f}%) >=50ms({slow_count/count*100:.1f}%)")
                    report_lines.append("")
                else:
                    market_emoji = "🏪" if market_type == "spot" else "📈"
                    market_name = "现货" if market_type == "spot" else "合约"
                    print(f"  {market_emoji} {market_name}: ❌ 未收到数据", flush=True)
                    report_lines.append(f"  {market_name}: 未收到数据")
        
        # 全局统计
        if all_latencies:
            global_avg = statistics.mean(all_latencies)
            global_median = statistics.median(all_latencies)
            global_std = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0
            
            print(f"\n🌍 全局延迟统计:", flush=True)
            print(f"   平均延迟: {global_avg:.3f}ms", flush=True)
            print(f"   中位延迟: {global_median:.3f}ms", flush=True)
            print(f"   标准差: {global_std:.3f}ms", flush=True)
            print(f"   总样本数: {len(all_latencies):,}", flush=True)
            
            report_lines.append("全局统计:")
            report_lines.append(f"  平均延迟: {global_avg:.3f}ms")
            report_lines.append(f"  中位延迟: {global_median:.3f}ms")
            report_lines.append(f"  总样本数: {len(all_latencies):,}")
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/reports/binance_websocket_latency_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 详细报告已保存: {filename}", flush=True)
        except Exception as e:
            print(f"❌ 保存报告失败: {e}", flush=True)
        
        print("\n🎉 测试完成！", flush=True)
        print("="*70, flush=True)

    async def run_test(self):
        """运行延迟测试"""
        print("🚀 " + "="*70, flush=True)
        print("🚀 币安Orderbook延迟测试工具 (WebSocket版本)", flush=True)
        print("🚀 " + "="*70, flush=True)
        print("🎯 测试币种: API3, OM, CFX", flush=True)
        print("📊 测试市场: 现货 + USDT永续合约", flush=True)
        print(f"⏱️  测试时长: {self.test_duration}秒", flush=True)
        print("📈 Orderbook深度: 5档", flush=True)
        print("🚀 " + "="*70, flush=True)
        print("正在连接WebSocket...", flush=True)
        
        self.start_time = time.time()
        
        # 创建任务
        tasks = [
            asyncio.create_task(self.connect_spot_websocket()),
            asyncio.create_task(self.connect_futures_websocket()),
            asyncio.create_task(self.print_stats_periodically()),
        ]
        
        # 设置测试结束定时器
        async def stop_test():
            await asyncio.sleep(self.test_duration)
            self.running = False
            print(f"\n⏰ {self.test_duration}秒测试时间到，正在停止...", flush=True)
        
        tasks.append(asyncio.create_task(stop_test()))
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            print("\n⏹️  收到中断信号，正在停止测试...", flush=True)
            self.running = False
        finally:
            # 生成最终报告
            self.generate_final_report()

    async def connect_spot_websocket(self):
        """连接现货WebSocket"""
        subscription = self.create_subscription_message(self.symbols)
        
        try:
            async with websockets.connect(self.spot_ws_url) as websocket:
                await websocket.send(subscription)
                print("✅ 现货WebSocket已连接", flush=True)
                await self.handle_websocket_messages(websocket, "spot")
        except Exception as e:
            print(f"❌ 现货WebSocket错误: {e}", flush=True)

    async def connect_futures_websocket(self):
        """连接合约WebSocket"""
        subscription = self.create_subscription_message(self.symbols)
        
        try:
            async with websockets.connect(self.futures_ws_url) as websocket:
                await websocket.send(subscription)
                print("✅ 合约WebSocket已连接", flush=True)
                await self.handle_websocket_messages(websocket, "futures")
        except Exception as e:
            print(f"❌ 合约WebSocket错误: {e}", flush=True)

    async def print_stats_periodically(self):
        """定期打印统计信息"""
        await asyncio.sleep(2)  # 等待连接建立
        
        while self.running:
            await asyncio.sleep(5)
            if self.start_time:
                self.print_current_stats()

    def print_current_stats(self):
        """打印当前统计信息"""
        elapsed = time.time() - self.start_time
        
        print(f"\n📈 实时延迟统计 - 运行时间: {elapsed:.1f}秒", flush=True)
        print(f"📊 总消息数: {self.total_messages:,}", flush=True)
        print("=" * 60, flush=True)
        
        for symbol in self.symbols:
            base_symbol = symbol.replace('USDT', '')
            print(f"\n💰 {base_symbol}:", flush=True)
            
            # 现货统计
            spot_key = f"{symbol}_spot"
            if spot_key in self.latencies and self.latencies[spot_key]:
                recent_latencies = self.latencies[spot_key][-20:]  # 最近20个数据点
                avg_lat = statistics.mean(recent_latencies)
                min_lat = min(recent_latencies)
                max_lat = max(recent_latencies)
                count = self.message_counts[spot_key]
                
                print(f"  🏪 现货: 消息={count:,}, 平均={avg_lat:.2f}ms, 范围={min_lat:.2f}-{max_lat:.2f}ms", flush=True)
            else:
                print("  🏪 现货: ⏳ 等待数据...", flush=True)
            
            # 合约统计
            futures_key = f"{symbol}_futures"
            if futures_key in self.latencies and self.latencies[futures_key]:
                recent_latencies = self.latencies[futures_key][-20:]
                avg_lat = statistics.mean(recent_latencies)
                min_lat = min(recent_latencies)
                max_lat = max(recent_latencies)
                count = self.message_counts[futures_key]
                
                print(f"  📈 合约: 消息={count:,}, 平均={avg_lat:.2f}ms, 范围={min_lat:.2f}-{max_lat:.2f}ms", flush=True)
            else:
                print("  📈 合约: ⏳ 等待数据...", flush=True)


async def main():
    """主函数"""
    print("🐳 Docker WebSocket 币安延迟测试", flush=True)
    print("="*50, flush=True)
    
    tester = BinanceWebSocketLatencyTester()
    await tester.run_test()


if __name__ == "__main__":
    try:
        print("🔍 检查依赖...", flush=True)
        import websockets
        print("✅ websockets库可用", flush=True)
    except ImportError:
        print("📦 安装websockets依赖...", flush=True)
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
        import websockets
        print("✅ websockets安装完成", flush=True)
    
    print("🚀 启动测试...", flush=True)
    asyncio.run(main())
