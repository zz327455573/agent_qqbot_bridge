#!/usr/bin/env python3
"""
agy-qq-bridge.py — AGY tmux 常驻进程直连 C2C 桥接 QQ
架构: QQ官方WS网关 ↔ Python asyncio ↔ tmux send-keys ↔ AGY
流程:
  QQ 消息 → 桥接脚本 → tmux send-keys -t 0 "消息" Enter
  AGY 回复 → 后台异步循环监听 AGY brain transcript.jsonl 增量推送到 QQ
"""
import asyncio
import json
import re
import os
import sys
import time
import uuid
import logging
import glob
from typing import Optional, Dict, Any
from pathlib import Path

# ================= 环境与配置加载 =================
def load_env(env_path: str = ".env"):
    """极简的本地 .env 解析函数，避免依赖外部 python-dotenv 库"""
    paths = [
        Path(env_path),
        Path(__file__).parent / env_path,
        Path.home() / ".env"
    ]
    for p in paths:
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            key, val = line.split("=", 1)
                            os.environ[key.strip()] = val.strip().strip('"').strip("'")
                break
            except Exception:
                pass

# 执行配置加载
load_env()

# ================= 配置区 =================
APP_ID = os.environ.get("APP_ID", "")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "")
MASTER_OPENID = os.environ.get("MASTER_OPENID", "")

TMUX_SESSION = os.environ.get("TMUX_SESSION", "0")
API_BASE = "https://api.sgroup.qq.com"
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
GATEWAY_URL_PATH = "/gateway"

CONNECT_TIMEOUT = 20
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 100
HEARTBEAT_INTERVAL = 15.0

# 路径与命令配置化（支持自定义配置以防本机硬编码）
BRAIN_DIR = Path(os.environ.get("BRAIN_DIR", str(Path.home() / ".gemini/antigravity-cli/brain")))
LOG_DIR = Path(os.environ.get("LOG_DIR", str(Path(__file__).parent / "logs")))
AGY_START_CMD = os.environ.get("AGY_START_CMD", "cd ~ && agy --dangerously-skip-permissions")
# ==========================================

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "agy-qq-bridge.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("agy_qq_bridge")

_access_token: Optional[str] = None
_token_expires_at: float = 0.0
_session_id: Optional[str] = None
_last_seq: Optional[int] = None
_ws = None
_http_client = None
_running = False
_last_msg_id: Optional[str] = None
_last_typing_sent_time = 0.0  # 记录上次发送“正在输入”通知的时间戳
_bot_openid: str = ""
heartbeat_task = None

# === 异步监听状态 ===
_last_log_size = 0
_current_log_path = None
_last_sent_timestamp = ""  # 记录最后发送给 QQ 的消息时间戳，防重与防历史刷屏


def find_latest_transcript(min_mtime: float) -> Optional[Path]:
    """获取在 min_mtime 之后新修改/创建的最新 transcript.jsonl 日志文件"""
    pattern = str(BRAIN_DIR / "*" / ".system_generated" / "logs" / "transcript.jsonl")
    paths = glob.glob(pattern)
    if not paths:
        return None
    paths_with_mtime = []
    for p in paths:
        try:
            mtime = os.path.getmtime(p)
            if mtime >= min_mtime:
                paths_with_mtime.append((Path(p), mtime))
        except OSError:
            continue
    if not paths_with_mtime:
        return None
    paths_with_mtime.sort(key=lambda x: x[1], reverse=True)
    return paths_with_mtime[0][0]


async def send_to_agy(message: str):
    """发送消息给 tmux 中的 AGY"""
    logger.info(f"[Tmux Target] Sending keys to session: {TMUX_SESSION}")
    # 模拟按 Escape 强退可能卡在 TUI 或 PAGER 的状态
    proc_esc = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", f"{TMUX_SESSION}:", "Escape", ""
    )
    await proc_esc.communicate()
    await asyncio.sleep(0.5)
    
    # 写入消息
    proc_msg = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", f"{TMUX_SESSION}:", message, ""
    )
    await proc_msg.communicate()
    await asyncio.sleep(0.1)
    
    # 按回车执行
    proc_enter = await asyncio.create_subprocess_exec(
        "tmux", "send-keys", "-t", f"{TMUX_SESSION}:", "Enter", ""
    )
    await proc_enter.communicate()
    logger.info(f"[Bridge -> AGY] {message[:100]}")


async def log_listener():
    """纯异步增量日志广播协程：无脑在后台读取最新修改日志的增量并推送到 QQ。"""
    global _current_log_path, _last_log_size, _last_sent_timestamp, _last_typing_sent_time
    
    # 启动时，先扫描并绑定目前最新的日志（以当前 24 小时前为基线）
    init_log = find_latest_transcript(time.time() - 86400.0)
    if init_log:
        _current_log_path = init_log
        try:
            _last_log_size = init_log.stat().st_size
            # 扫描已有的历史日志，提取最新一条回复的时间戳，进行时间锁死防止历史刷屏
            with open(init_log, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.read().splitlines()
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("type") == "PLANNER_RESPONSE" and obj.get("source") == "MODEL":
                        ts = obj.get("created_at")
                        if ts:
                            _last_sent_timestamp = ts
                            break
                except Exception:
                    continue
        except OSError:
            _last_log_size = 0
        logger.info(f"[Listener] Bound to existing active log: {_current_log_path} (size={_last_log_size}, last_ts={_last_sent_timestamp})")

    while _running:
        await asyncio.sleep(0.5)
        
        # 1. 动态探测是否有新修改的文件诞生（比如重置会话拉起新 UUID 目录）
        try:
            latest_log = find_latest_transcript(time.time() - 86400.0)
            if latest_log and (not _current_log_path or latest_log != _current_log_path):
                _current_log_path = latest_log
                _last_log_size = 0  # 绑定全新文件，从头读起
                logger.info(f"[Listener] Switched to newer active log: {_current_log_path}")
        except Exception as e:
            logger.error(f"[Listener] Scan error: {e}")

        if not _current_log_path:
            continue

        # 2. 检测大小变动
        try:
            curr_size = _current_log_path.stat().st_size
        except FileNotFoundError:
            _current_log_path = None
            continue

        # 针对日志文件被 AI 客户端自动截断/收缩导致的 log rotation 现象进行安全水位重置
        if curr_size < _last_log_size:
            logger.info(f"[Listener] Log file truncated (decreased from {_last_log_size} to {curr_size}), resetting offset.")
            _last_log_size = 0

        if curr_size <= _last_log_size:
            continue

        # 触发/续杯“正在输入中”的顶部状态
        now = time.time()
        if _last_msg_id and (now - _last_typing_sent_time > 5.0):
            asyncio.create_task(send_input_notify(MASTER_OPENID, _last_msg_id))
            _last_typing_sent_time = now

        # 3. 增量读取新行
        try:
            with open(_current_log_path, 'r', encoding='utf-8', errors='replace') as f:
                f.seek(_last_log_size)
                new_lines = f.read().splitlines()
        except OSError:
            continue

        # 更新指针
        _last_log_size = curr_size

        # 4. 解析增量行
        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            
            # 只捕获模型返回的最终回复内容
            if obj.get("type") == "PLANNER_RESPONSE" and obj.get("source") == "MODEL":
                ts = obj.get("created_at")
                # 如果当前行的时间戳不大于已发送的时间戳，说明是重读的历史记录，直接跳过
                if ts and _last_sent_timestamp and ts <= _last_sent_timestamp:
                    continue
                
                content = obj.get("content", "")
                if isinstance(content, list):
                    text = "\n".join(
                        item.get("text", "") for item in content
                        if isinstance(item, dict) and item.get("type") == "text"
                    )
                else:
                    text = str(content)
                text = text.strip()
                if text:
                    logger.info(f"[Listener -> QQ] Broadcasting response (ts={ts}): {text[:100]}")
                    if ts:
                        _last_sent_timestamp = ts
                    await send_message_rest(MASTER_OPENID, text)


async def send_message_rest(user_openid: str, content: str) -> bool:
    """给指定用户发送 C2C 消息"""
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "AGY-QQ-Bridge/2.0",
    }
    msg_seq = _next_msg_seq(user_openid)
    display_content = content[:3990] + "\n\n... (已截断)" if len(content) > 4000 else content
    body = {"markdown": {"content": display_content}, "msg_type": 2, "msg_seq": msg_seq}

    try:
        resp = await client.post(
            f"{API_BASE}/v2/users/{user_openid}/messages",
            headers=headers, json=body, timeout=30.0,
        )
        if resp.status_code >= 400:
            logger.error(f"Send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Send exception: {e}")
        return False


async def send_input_notify(user_openid: str, msg_id: str) -> bool:
    """给指定用户发送“正在输入”通知状态（msg_type: 6）"""
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "AGY-QQ-Bridge/2.0",
    }
    msg_seq = _next_msg_seq(user_openid)
    body = {
        "msg_type": 6,
        "input_notify": {"input_type": 1, "input_second": 10},
        "msg_seq": msg_seq,
        "msg_id": msg_id,
    }

    try:
        resp = await client.post(
            f"{API_BASE}/v2/users/{user_openid}/messages",
            headers=headers, json=body, timeout=30.0,
        )
        if resp.status_code >= 400:
            logger.error(f"Typing notify failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Typing notify exception: {e}")
        return False


def get_http_client():
    global _http_client
    if _http_client is None:
        import httpx
        _http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
    return _http_client


async def ensure_token() -> str:
    global _access_token, _token_expires_at
    if _access_token and time.time() < _token_expires_at - 60:
        return _access_token
    client = get_http_client()
    resp = await client.post(
        TOKEN_URL,
        json={"appId": APP_ID, "clientSecret": CLIENT_SECRET},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Failed to get token: {data}")
    expires_in = int(data.get("expires_in", 7200))
    _access_token = token
    _token_expires_at = time.time() + expires_in
    logger.info(f"Token refreshed, expires in {expires_in}s")
    return token


async def get_gateway_url() -> str:
    token = await ensure_token()
    client = get_http_client()
    resp = await client.get(
        f"{API_BASE}{GATEWAY_URL_PATH}",
        headers={"Authorization": f"QQBot {token}", "User-Agent": "AGY-QQ-Bridge/1.0"},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    url = data.get("url")
    if not url:
        raise RuntimeError(f"Failed to get gateway URL: {data}")
    return url


async def send_identify(ws):
    token = await ensure_token()
    payload = {
        "op": 2,
        "d": {
            "token": f"QQBot {token}",
            "intents": (1 << 25) | (1 << 30) | (1 << 12) | (1 << 26),
            "shard": [0, 1],
            "properties": {"$os": "Linux", "$browser": "agy-qq-bridge", "$device": "agy-qq-bridge"},
        },
    }
    await ws.send_json(payload)
    logger.info("Identify sent")


async def send_resume(ws):
    token = await ensure_token()
    payload = {
        "op": 6,
        "d": {"token": f"QQBot {token}", "session_id": _session_id, "seq": _last_seq},
    }
    await ws.send_json(payload)
    logger.info(f"Resume sent (session={_session_id}, seq={_last_seq})")


def _next_msg_seq(msg_id: str = "default") -> int:
    time_part = int(time.time()) % 100000000
    rand = int(uuid.uuid4().hex[:4], 16)
    return (time_part ^ rand) % 65536


_seen_messages: Dict[str, float] = {}


def is_duplicate(msg_id: str) -> bool:
    now = time.time()
    if msg_id in _seen_messages and now - _seen_messages[msg_id] < 300:
        return True
    _seen_messages[msg_id] = now
    if len(_seen_messages) > 1000:
        for k in list(_seen_messages.keys()):
            if now - _seen_messages[k] > 600:
                del _seen_messages[k]
    return False


async def handle_c2c_message(d: dict):
    global _last_msg_id, _bot_openid

    msg_id = str(d.get("id", ""))
    if not msg_id or is_duplicate(msg_id):
        return

    content = str(d.get("content", "")).strip()
    
    # 提取附件 URL（图片、语音、视频、文件等），零截留原样透传
    attachments = d.get("attachments") or []
    for att in attachments:
        url = att.get("url")
        if url:
            name = att.get("filename") or att.get("name") or "file"
            content += f"\n\n[附件({name}): {url}]"
            
    content = content.strip()
    author = d.get("author") if isinstance(d.get("author"), dict) else {}
    user_openid = str(author.get("user_openid", ""))

    if not user_openid or not content:
        return

    _last_msg_id = msg_id
    logger.info(f"[Recv] openid={user_openid}: {content[:100]}")

    if user_openid != MASTER_OPENID:
        logger.info(f"[Skip] non-master openid: {user_openid}")
        return

    # 命令处理
    if content.strip().lower() in ["/new", "/reset", "/清空", "/新对话"]:
        logger.info("[Recv] New session command received")
        
        # 1. 强杀 tmux s0
        proc_kill = await asyncio.create_subprocess_shell(f"tmux kill-session -t {TMUX_SESSION} 2>/dev/null || true")
        await proc_kill.communicate()
        await asyncio.sleep(0.5)
        
        # 2. 强建 tmux s0
        proc_new = await asyncio.create_subprocess_exec("tmux", "new-session", "-d", "-s", TMUX_SESSION)
        await proc_new.communicate()
        await asyncio.sleep(2.0)
        
        # 3. 启动 AGY
        proc_start = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", f"{TMUX_SESSION}:", AGY_START_CMD, "Enter"
        )
        await proc_start.communicate()

        # 4. 确认信任提示
        await asyncio.sleep(4.0)
        proc_enter = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", f"{TMUX_SESSION}:", "Enter", ""
        )
        await proc_enter.communicate()
        
        reply = "✅ 已强杀并重建 tmux 会话，重新拉起全新 AGY。上下文已完全重置。"
        await send_message_rest(user_openid, reply)
        return

    if content.strip().lower() in ["/stop", "/停止", "/kill"]:
        logger.info("[Recv] Stop command received")
        for key in ["C-c", "Enter", "C-c"]:
            proc = await asyncio.create_subprocess_exec(
                "tmux", "send-keys", "-t", f"{TMUX_SESSION}:", key, ""
            )
            await proc.communicate()
            await asyncio.sleep(0.3)
        reply = "⛔ 已发送终止信号并尝试恢复命令行。"
        await send_message_rest(user_openid, reply)
        return

    logger.info(f"[QQ -> AGY] {content}")
    # 直接发送，不等待，不阻塞
    await send_to_agy(content)
    # 立即触发一次“正在输入中”的状态
    await send_input_notify(user_openid, msg_id)


async def event_loop(ws):
    global _session_id, _last_seq, _running, _ws, heartbeat_task
    _ws = ws
    heartbeat_interval = HEARTBEAT_INTERVAL
    heartbeat_task = asyncio.create_task(_heartbeat_sender(ws, heartbeat_interval))

    try:
        while _running and ws and not ws.closed:
            msg = await ws.receive()
            if msg.type == 1:
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    logger.warning(f"JSON parse error: {msg.data[:100]}")
                    continue

                op = payload.get("op")
                t = payload.get("t")
                s = payload.get("s")
                d = payload.get("d")

                if isinstance(s, int) and (_last_seq is None or s > _last_seq):
                    _last_seq = s

                if op == 10:
                    d_data = d if isinstance(d, dict) else {}
                    interval_ms = d_data.get("heartbeat_interval", 30000)
                    heartbeat_interval = interval_ms / 1000.0 * 0.8
                    logger.info(f"Hello recv, heartbeat={heartbeat_interval:.1f}s")
                    if _session_id and _last_seq is not None:
                        await send_resume(ws)
                    else:
                        await send_identify(ws)
                    continue

                if op == 0 and t:
                    logger.info(f"[WS Dispatch] event_type={t}")
                    if t == "READY":
                        if isinstance(d, dict):
                            global _bot_openid
                            _session_id = d.get("session_id")
                            user = d.get("user") if isinstance(d.get("user"), dict) else {}
                            _bot_openid = str(user.get("id", ""))
                            logger.info(f"READY, session_id={_session_id}, bot_openid={_bot_openid}")
                    elif t == "RESUMED":
                        logger.info("Session resumed")
                    elif t == "C2C_MESSAGE_CREATE":
                        task = asyncio.create_task(handle_c2c_message(d))
                        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                    continue

            elif msg.type == 9:
                logger.warning("WS close received")
                break

    except Exception as e:
        logger.error(f"Event loop error: {e}")


async def _heartbeat_sender(ws, interval: float):
    try:
        while _running and ws and not ws.closed:
            await asyncio.sleep(interval)
            if ws and not ws.closed:
                await ws.send_json({"op": 1, "d": _last_seq})
                logger.debug("Heartbeat sent")
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.debug(f"Heartbeat error: {e}")


async def main():
    global _running
    _running = True

    # 启动后台异步日志监听服务
    asyncio.create_task(log_listener())

    try:
        gateway_url = await get_gateway_url()
        logger.info(f"Gateway URL: {gateway_url}")
    except Exception as e:
        logger.error(f"Failed to get gateway: {e}")
        sys.exit(1)

    import aiohttp

    while _running:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    gateway_url,
                    timeout=aiohttp.ClientTimeout(total=CONNECT_TIMEOUT),
                    heartbeat=HEARTBEAT_INTERVAL,
                ) as ws:
                    logger.info("WS connected")
                    await event_loop(ws)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if _running:
                logger.error(f"WS connection error: {e}")
                backoff = RECONNECT_BACKOFF[0]
                logger.info(f"Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)

    logger.info("Bridge stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")