import asyncio
import json
import logging
import os
import statistics
import time
from collections import OrderedDict
from urllib.parse import urlparse

import websockets


chinese_time_format = "%Y年%m月%d日%H时%M分%S秒"
log_format = "[%(levelname)s] %(asctime)s%(msecs)03d毫秒 [%(name)s]: %(message)s"
logging.basicConfig(
    format=log_format, level=logging.INFO, datefmt=chinese_time_format, force=True
)
logger = logging.getLogger(__name__)

WINDOW_BLOCKS = int(os.getenv("BSC_WSS_COMPARE_WINDOW_BLOCKS", "100"))
SETTLE_SECONDS = float(os.getenv("BSC_WSS_COMPARE_SETTLE_SECONDS", "2"))
RECONNECT_SECONDS = float(os.getenv("BSC_WSS_RECONNECT_SECONDS", "3"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("BSC_WSS_CONNECT_TIMEOUT_SECONDS", "15"))
PING_INTERVAL_SECONDS = float(os.getenv("BSC_WSS_PING_INTERVAL_SECONDS", "20"))
PING_TIMEOUT_SECONDS = float(os.getenv("BSC_WSS_PING_TIMEOUT_SECONDS", "10"))


def split_env_list(value):
    if not value:
        return []
    normalized = value.replace("\n", ",").replace(";", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def default_node_name(url, index):
    parsed = urlparse(url)
    host = parsed.netloc or f"node-{index}"
    return f"{index}:{host}"


def parse_nodes():
    raw_urls = split_env_list(os.getenv("BSC_WSS_URLS"))
    names = split_env_list(os.getenv("BSC_WSS_NAMES"))

    nodes = []
    for index, item in enumerate(raw_urls, start=1):
        if "=" in item and not item.startswith(("ws://", "wss://")):
            name, url = item.split("=", 1)
            nodes.append((name.strip(), url.strip()))
            continue
        name = (
            names[index - 1] if index <= len(names) else default_node_name(item, index)
        )
        nodes.append((name, item))

    index = 1
    while True:
        value = os.getenv(f"BSC_WSS_URL_{index}")
        if not value:
            break
        name = os.getenv(f"BSC_WSS_NAME_{index}", default_node_name(value, index))
        nodes.append((name, value.strip()))
        index += 1

    fallback_url = os.getenv("BSC_WSS_URL")
    if fallback_url and not nodes:
        nodes.append(
            (
                os.getenv("BSC_WSS_NAME", default_node_name(fallback_url, 1)),
                fallback_url,
            )
        )

    seen = set()
    unique_nodes = []
    for name, url in nodes:
        if not url or url in seen:
            continue
        seen.add(url)
        unique_nodes.append((name, url))
    return unique_nodes


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percent / 100))
    return ordered[index]


class BlockLatencyCollector:
    def __init__(self, nodes, window_blocks, settle_seconds):
        self.node_names = [name for name, _url in nodes]
        self.window_blocks = window_blocks
        self.settle_seconds = settle_seconds
        self.blocks = OrderedDict()
        self.reported_count = 0
        self.window_number = 0
        self.lock = asyncio.Lock()

    async def record(self, node_name, block_number, block_hash, arrival_ns):
        async with self.lock:
            block = self.blocks.setdefault(
                block_number,
                {"hashes": {}, "arrivals": {}, "first_seen_ns": arrival_ns},
            )
            block["hashes"][node_name] = block_hash
            block["arrivals"].setdefault(node_name, arrival_ns)
            ready_windows = (
                len(self.blocks) - self.reported_count
            ) // self.window_blocks
            if ready_windows <= 0:
                return

            tasks = []
            for _ in range(ready_windows):
                start = self.reported_count
                end = start + self.window_blocks
                self.reported_count = end
                self.window_number += 1
                tasks.append(
                    asyncio.create_task(
                        self.report_after_settle(start, end, self.window_number)
                    )
                )

        for task in tasks:
            task.add_done_callback(self.log_task_error)

    @staticmethod
    def log_task_error(task):
        try:
            task.result()
        except Exception as e:
            logger.exception("统计窗口任务失败: %s", e)

    async def report_after_settle(self, start, end, window_number):
        await asyncio.sleep(self.settle_seconds)
        async with self.lock:
            block_items = list(self.blocks.items())[start:end]
        self.print_report(window_number, block_items)

    def print_report(self, window_number, block_items):
        stats = {
            name: {
                "first": 0,
                "observed": 0,
                "missing": 0,
                "delays_ms": [],
            }
            for name in self.node_names
        }

        for _block_number, block in block_items:
            arrivals = block["arrivals"]
            if not arrivals:
                continue
            fastest_node, fastest_ns = min(arrivals.items(), key=lambda item: item[1])
            stats[fastest_node]["first"] += 1
            for node_name in self.node_names:
                arrival_ns = arrivals.get(node_name)
                if arrival_ns is None:
                    stats[node_name]["missing"] += 1
                    continue
                stats[node_name]["observed"] += 1
                stats[node_name]["delays_ms"].append(
                    (arrival_ns - fastest_ns) / 1_000_000
                )

        rows = []
        for node_name, item in stats.items():
            delays = item["delays_ms"]
            avg_ms = statistics.fmean(delays) if delays else None
            p50_ms = statistics.median(delays) if delays else None
            p95_ms = percentile(delays, 95)
            rows.append(
                {
                    "node": node_name,
                    "first": item["first"],
                    "observed": item["observed"],
                    "missing": item["missing"],
                    "avg_ms": avg_ms,
                    "p50_ms": p50_ms,
                    "p95_ms": p95_ms,
                }
            )

        rows.sort(
            key=lambda row: (
                -row["first"],
                row["avg_ms"] if row["avg_ms"] is not None else float("inf"),
                row["missing"],
                row["node"],
            )
        )

        first_block = block_items[0][0] if block_items else "?"
        last_block = block_items[-1][0] if block_items else "?"
        lines = [
            "",
            f"===== BSC WSS 延迟统计 #{window_number} 区块 {first_block}-{last_block} "
            f"({len(block_items)} blocks, settle={self.settle_seconds:g}s) =====",
            "rank  first  observed  missing  avg_ms   p50_ms   p95_ms   node",
            "----  -----  --------  -------  -------  -------  -------  ----",
        ]
        for rank, row in enumerate(rows, start=1):
            lines.append(
                f"{rank:>4}  "
                f"{row['first']:>5}  "
                f"{row['observed']:>8}  "
                f"{row['missing']:>7}  "
                f"{format_ms(row['avg_ms']):>7}  "
                f"{format_ms(row['p50_ms']):>7}  "
                f"{format_ms(row['p95_ms']):>7}  "
                f"{row['node']}"
            )
        logger.info("\n%s", "\n".join(lines))


def format_ms(value):
    if value is None:
        return "-"
    return f"{value:.1f}"


async def subscribe_new_heads(node_name, url, collector):
    subscribe_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newHeads"],
    }

    while True:
        try:
            async with websockets.connect(
                url,
                open_timeout=CONNECT_TIMEOUT_SECONDS,
                ping_interval=PING_INTERVAL_SECONDS,
                ping_timeout=PING_TIMEOUT_SECONDS,
                close_timeout=10,
            ) as ws:
                await ws.send(json.dumps(subscribe_request))
                response = json.loads(await ws.recv())
                if "error" in response:
                    raise RuntimeError(response["error"])
                logger.info(
                    "已连接并订阅 %s subscription_id=%s",
                    node_name,
                    response.get("result"),
                )

                async for message in ws:
                    arrival_ns = time.perf_counter_ns()
                    data = json.loads(message)
                    if data.get("method") != "eth_subscription":
                        continue
                    header = data.get("params", {}).get("result", {})
                    number_hex = header.get("number")
                    if not number_hex:
                        continue
                    block_number = int(number_hex, 16)
                    block_hash = header.get("hash")
                    await collector.record(
                        node_name, block_number, block_hash, arrival_ns
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(
                "%s 连接/订阅失败，%.1fs 后重连: %s", node_name, RECONNECT_SECONDS, e
            )
            await asyncio.sleep(RECONNECT_SECONDS)


async def main():
    nodes = parse_nodes()
    if len(nodes) < 2:
        raise SystemExit(
            "请通过 BSC_WSS_URLS 配置至少 2 个 WSS 节点，示例: "
            'BSC_WSS_URLS="nodeA=wss://...,nodeB=wss://..."'
        )

    logger.info(
        "准备比较 %d 个 BSC WSS 节点，每 %d 个区块输出一次", len(nodes), WINDOW_BLOCKS
    )
    for name, url in nodes:
        logger.info("节点: %s -> %s", name, url)

    collector = BlockLatencyCollector(nodes, WINDOW_BLOCKS, SETTLE_SECONDS)
    await asyncio.gather(
        *(subscribe_new_heads(name, url, collector) for name, url in nodes)
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，退出")
