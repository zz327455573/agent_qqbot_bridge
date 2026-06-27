#!/usr/bin/env python3
"""
agy-qq-bridge.py — AGY tmux 常驻进程桥接 QQ
架构: QQ官方WS网关 ↔ Python asyncio ↔ tmux send-keys/capture-pane ↔ AGY

流程:
  QQ 消息 → 桥接脚本 → tmux send-keys -t 0 "消息" Enter
  AGY 回复 → 读取 AGY brain transcript.jsonl (PLANNER_RESPONSE)
  审批TUI → tmux capture-pane 检测 → QQ 推送审批卡片

启动: python3 /root/agy_workspace/scripts/agy-qq-bridge.py
依赖: pip install aiohttp httpx
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

# ================= 配置区 =================
APP_ID = "1903830759"
CLIENT_SECRET = "RyW5eEoP0cErU8nS8oVCucL4oZK6sfSG"
MASTER_OPENID = "FF86A54C2DFDD5A7E7B18DE4BCA2DB63"

TMUX_SESSION = "0"
API_BASE = "https://api.sgroup.qq.com"
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
GATEWAY_URL_PATH = "/gateway"

CONNECT_TIMEOUT = 20
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 100
HEARTBEAT_INTERVAL = 15.0
# ==========================================

os.makedirs("/root/agy_workspace/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/root/agy_workspace/logs/agy-qq-bridge.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("agy_qq_bridge")

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

_access_token: Optional[str] = None
_token_expires_at: float = 0.0
_session_id: Optional[str] = None
_last_seq: Optional[int] = None
_ws = None
_http_client = None
_running = False
_last_msg_id: Optional[str] = None
_bot_openid: str = ""
heartbeat_task = None
_processing = False
_current_approval_key = None

BRAIN_DIR = Path("/root/.gemini/antigravity-cli/brain")
_LATEST_TRANSCRIPT = None


def _get_transcript_path() -> str:
    global _LATEST_TRANSCRIPT
    if _LATEST_TRANSCRIPT:
        return _LATEST_TRANSCRIPT
    pattern = str(BRAIN_DIR / "*" / ".system_generated" / "logs" / "transcript.jsonl")
    paths = glob.glob(pattern)
    if not paths:
        return ""
    _LATEST_TRANSCRIPT = max(paths, key=os.path.getmtime)
    return _LATEST_TRANSCRIPT


def clean_ansi(text: str) -> str:
    text = ANSI_ESCAPE.sub('', text)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        if '\r' in line:
            line = line.split('\r')[-1]
        cleaned.append(line)
    return '\n'.join(cleaned).strip()


def strip_prompt_lines(text: str) -> str:
    lines = text.split('\n')
    result = []
    for line in lines:
        if line.strip().startswith('─' * 10):
            continue
        if '? for shortcuts' in line:
            continue
        if ('Gemini' in line or 'Antigravity' in line) and 'for shortcuts' in line:
            continue
        if not line.strip():
            continue
        result.append(line)
    return '\n'.join(result).strip()


async def send_to_agy(message: str):
    escaped = message.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")
    cmd = f'tmux send-keys -t {TMUX_SESSION} "{escaped}" Enter'
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    logger.info(f"[Bridge -> AGY] {message[:100]}")


async def wait_for_agy_response(timeout=300) -> str:
    """等待 AGY 回复，同时监控审批 TUI。

    正常回复：从 AGY brain transcript.jsonl 读取 PLANNER_RESPONSE
    审批 TUI ：tmux capture-pane 检测 → 返回 __APPROVAL_REQUIRED__
    """
    transcript = _get_transcript_path()
    if not transcript:
        return "[AGY: 找不到 transcript 文件]"

    try:
        watermark = Path(transcript).stat().st_size
    except FileNotFoundError:
        return "[AGY: transcript 文件不存在]"

    start = time.time()

    while time.time() - start < timeout:
        await asyncio.sleep(0.5)

        # 1. 检测审批 TUI（读终端）
        proc = await asyncio.create_subprocess_exec(
            "tmux", "capture-pane", "-t", TMUX_SESSION, "-p",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        pane = clean_ansi(stdout.decode("utf-8", errors="replace"))
        if is_permission_prompt(pane):
            # 先检查transcript有没有新内容——有的话说明AGY已经在处理了
            try:
                current_size = Path(transcript).stat().st_size
                if current_size > watermark:
                    # transcript有新内容，审批已自动通过，继续读回复
                    continue
            except:
                pass
            # transcript没动，再走轮询等TUI消失逻辑
            for _ in range(20):  # 每 0.5s check 一次，最多 10 秒
                await asyncio.sleep(0.5)
                proc2 = await asyncio.create_subprocess_exec(
                    "tmux", "capture-pane", "-t", TMUX_SESSION, "-p",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout2, _ = await proc2.communicate()
                pane2 = clean_ansi(stdout2.decode("utf-8", errors="replace"))
                if not is_permission_prompt(pane2):
                    # TUI 已消失，自动通过了，继续等 transcript
                    break
            else:
                # 10 秒后 TUI 还在，才是真正需要用户审批
                return "__APPROVAL_REQUIRED__"
            continue

        # 2. 检测 transcript 新回复
        try:
            current_size = Path(transcript).stat().st_size
        except FileNotFoundError:
            continue
        if current_size <= watermark:
            continue

        with open(transcript, 'r', encoding='utf-8', errors='replace') as f:
            f.seek(watermark)
            new_lines = f.read().splitlines()

        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("type") == "PLANNER_RESPONSE" and obj.get("source") == "MODEL":
                content = obj.get("content", "")
                if isinstance(content, list):
                    text = "\n".join(
                        item.get("text", "") for item in content
                        if isinstance(item, dict) and item.get("type") == "text"
                    )
                else:
                    text = str(content)
                if text.strip():
                    return text.strip()

        watermark = current_size

    return "[AGY 超时无回复]"


def is_permission_prompt(text: str) -> bool:
    """检测 AGY CLI 终端审批 TUI 是否出现。只检查末尾10行，避免历史回复误判。"""
    lines = text.splitlines()
    if len(lines) > 10:
        text = "\n".join(lines[-10:])
    markers = [
        "Requesting permission for:",
        "Do you want to proceed?",
        "↑/↓ Navigate · tab Amend",
    ]
    return any(m in text for m in markers)


async def send_message_rest(user_openid: str, content: str) -> bool:
    global _current_approval_key
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "AGY-QQ-Bridge/1.0",
    }
    msg_seq = _next_msg_seq(user_openid)
    display_content = content[:3990] + "\n\n... (已截断)" if len(content) > 4000 else content
    body = {"markdown": {"content": display_content}, "msg_type": 2, "msg_seq": msg_seq}

    if is_permission_prompt(content):
        _current_approval_key = uuid.uuid4().hex[:16]
        logger.info(f"Generated approval key: {_current_approval_key} for user: {user_openid}")
        body["keyboard"] = {
            "content": {
                "rows": [
                    {
                        "buttons": [
                            {
                                "id": "btn_allow",
                                "render_data": {"label": "✅ 允许一次", "visited_label": "已允许", "style": 1},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:allow"},
                            },
                            {
                                "id": "btn_allow_similar",
                                "render_data": {"label": "⚡ 本次允许同类", "visited_label": "已允许同类", "style": 1},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:allow_similar"},
                            },
                        ]
                    },
                    {
                        "buttons": [
                            {
                                "id": "btn_allow_always",
                                "render_data": {"label": "🛡️ 永久允许", "visited_label": "已永久允许", "style": 1},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:allow_always"},
                            },
                            {
                                "id": "btn_deny",
                                "render_data": {"label": "❌ 拒绝", "visited_label": "已拒绝", "style": 0},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:deny"},
                            },
                        ]
                    },
                ]
            }
        }

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


async def send_group_message_rest(group_openid: str, content: str, reply_to: Optional[str] = None) -> bool:
    global _current_approval_key
    token = await ensure_token()
    client = get_http_client()
    headers = {
        "Authorization": f"QQBot {token}",
        "Content-Type": "application/json",
        "User-Agent": "AGY-QQ-Bridge/1.0",
    }
    msg_seq = _next_msg_seq(group_openid)
    display_content = content[:3990] + "\n\n... (已截断)" if len(content) > 4000 else content
    body = {"markdown": {"content": display_content}, "msg_type": 2, "msg_seq": msg_seq}
    if reply_to:
        body["msg_id"] = reply_to

    if is_permission_prompt(content):
        _current_approval_key = uuid.uuid4().hex[:16]
        logger.info(f"Generated approval key: {_current_approval_key} for group: {group_openid}")
        body["keyboard"] = {
            "content": {
                "rows": [
                    {
                        "buttons": [
                            {
                                "id": "btn_allow",
                                "render_data": {"label": "✅ 允许一次", "visited_label": "已允许", "style": 1},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:allow"},
                            },
                            {
                                "id": "btn_allow_similar",
                                "render_data": {"label": "⚡ 本次允许同类", "visited_label": "已允许同类", "style": 1},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:allow_similar"},
                            },
                        ]
                    },
                    {
                        "buttons": [
                            {
                                "id": "btn_allow_always",
                                "render_data": {"label": "🛡️ 永久允许", "visited_label": "已永久允许", "style": 1},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:allow_always"},
                            },
                            {
                                "id": "btn_deny",
                                "render_data": {"label": "❌ 拒绝", "visited_label": "已拒绝", "style": 0},
                                "action": {"type": 2, "permission": {"type": 2, "specify_user_ids": [MASTER_OPENID]}, "data": f"approve:{_current_approval_key}:deny"},
                            },
                        ]
                    },
                ]
            }
        }

    try:
        resp = await client.post(
            f"{API_BASE}/v2/groups/{group_openid}/messages",
            headers=headers, json=body, timeout=30.0,
        )
        if resp.status_code >= 400:
            logger.error(f"Group send failed [{resp.status_code}]: {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        logger.error(f"Group send exception: {e}")
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


GROUP_CONTEXT_CACHE: Dict[str, list] = {}


async def handle_group_message(d: dict, event_type: str):
    global _last_msg_id, _processing, _bot_openid, GROUP_CONTEXT_CACHE

    msg_id = str(d.get("id", ""))
    logger.info(f"[Group Raw] event={event_type} msg_id={msg_id} group={d.get('group_openid')} content={d.get('content')} mentions={d.get('mentions')}")
    if not msg_id or is_duplicate(msg_id):
        return

    group_openid = str(d.get("group_openid", ""))
    content = str(d.get("content", "")).strip()
    author = d.get("author") if isinstance(d.get("author"), dict) else {}
    member_openid = str(author.get("member_openid", ""))

    if not group_openid or not content:
        return

    sender_name = author.get("nickname") or author.get("username")
    if not sender_name:
        sender_name = f"user_{member_openid[-6:]}" if member_openid else "User"
    msg_line = f"[{sender_name}] {content.strip()}"

    is_mentioned = False
    my_openid_in_group = ""

    mentions = d.get("mentions") or []
    for m in mentions:
        if m.get("is_you") is True:
            my_openid_in_group = m.get("member_openid") or m.get("id") or m.get("user_openid") or ""
            break

    if event_type == "GROUP_AT_MESSAGE_CREATE":
        is_mentioned = True
    elif event_type == "GROUP_MESSAGE_CREATE":
        if my_openid_in_group:
            is_mentioned = True
        elif _bot_openid and mentions:
            for m in mentions:
                mid = m.get("member_openid") or m.get("id") or m.get("user_openid") or ""
                if str(mid) == str(_bot_openid):
                    is_mentioned = True
                    break

    if not is_mentioned:
        if group_openid not in GROUP_CONTEXT_CACHE:
            GROUP_CONTEXT_CACHE[group_openid] = []
        GROUP_CONTEXT_CACHE[group_openid].append(msg_line)
        GROUP_CONTEXT_CACHE[group_openid] = GROUP_CONTEXT_CACHE[group_openid][-100:]
        return

    _last_msg_id = msg_id
    logger.info(f"[Group Recv] group={group_openid} member={member_openid}: {content[:100]}")

    if member_openid != MASTER_OPENID:
        logger.info(f"[Group Skip] non-master openid: {member_openid}")
        if group_openid not in GROUP_CONTEXT_CACHE:
            GROUP_CONTEXT_CACHE[group_openid] = []
        GROUP_CONTEXT_CACHE[group_openid].append(msg_line)
        GROUP_CONTEXT_CACHE[group_openid] = GROUP_CONTEXT_CACHE[group_openid][-100:]
        return

    cleaned_content = content
    if my_openid_in_group:
        cleaned_content = re.sub(rf"<@!?{my_openid_in_group}>", "", cleaned_content).strip()
    if _bot_openid:
        cleaned_content = re.sub(rf"<@!?{_bot_openid}>", "", cleaned_content).strip()

    if not cleaned_content:
        return

    if cleaned_content.startswith("approve:"):
        parts = cleaned_content.split(":")
        if len(parts) >= 3:
            key = parts[1]
            decision = parts[2]
            asyncio.create_task(handle_approval_action(key, decision, member_openid, group_openid, msg_id))
            return

    if cleaned_content.lower() in ["/new", "/reset", "/清空", "/新对话"]:
        logger.info("[Group Recv] New session command received")
        await send_to_agy("/new")
        reply = "✅ AGY 会话已重置，开始新对话。"
        await send_group_message_rest(group_openid, reply, reply_to=msg_id)
        return

    if cleaned_content.lower() in ["/stop", "/停止", "/kill"]:
        logger.info("[Group Recv] Stop command received")
        await send_to_agy("/new")
        reply = "🛑 已重置 AGY 会话。"
        await send_group_message_rest(group_openid, reply, reply_to=msg_id)
        return

    if _processing:
        await send_group_message_rest(group_openid, "⚠️ 当前已有任务正在执行中，请稍候。", reply_to=msg_id)
        return

    channel_context = ""
    if group_openid in GROUP_CONTEXT_CACHE:
        history_lines = GROUP_CONTEXT_CACHE[group_openid]
        if history_lines:
            channel_context = "[Recent group chat context]\n" + "\n".join(history_lines)
            GROUP_CONTEXT_CACHE[group_openid] = []

    prompt_to_send = cleaned_content
    if channel_context:
        prompt_to_send = f"{channel_context}\n\n[New message]\n{cleaned_content}"

    logger.info(f"[QQ Group -> AGY] group={group_openid}: {cleaned_content[:100]}")

    _processing = True
    try:
        await send_group_message_rest(group_openid, "⏳ AGY 正在思考...", reply_to=msg_id)
        await send_to_agy(prompt_to_send)
        reply = await wait_for_agy_response()
        if reply == "__APPROVAL_REQUIRED__":
            # 重新 capture 获取审批内容，有按钮卡片
            proc = await asyncio.create_subprocess_exec("tmux", "capture-pane", "-t", TMUX_SESSION, "-p", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            pane_content = clean_ansi(stdout.decode("utf-8", errors="replace"))
            reply = strip_prompt_lines(pane_content)
        if not reply:
            reply = "[AGY 无回复]"
    except Exception as e:
        reply = f"[AGY error] {str(e)[:300]}"
        logger.error(f"Error processing group message: {e}")
    finally:
        _processing = False

    logger.info(f"[AGY -> QQ Group] {reply[:200]}")
    success = await send_group_message_rest(group_openid, reply, reply_to=msg_id)
    if not success:
        logger.error("Group reply send failed")


async def handle_c2c_message(d: dict):
    global _last_msg_id, _processing, _bot_openid

    msg_id = str(d.get("id", ""))
    if not msg_id or is_duplicate(msg_id):
        return

    content = str(d.get("content", "")).strip()
    author = d.get("author") if isinstance(d.get("author"), dict) else {}
    user_openid = str(author.get("user_openid", ""))

    if not user_openid or not content:
        return

    _last_msg_id = msg_id
    logger.info(f"[Recv] openid={user_openid}: {content[:100]}")

    if user_openid != MASTER_OPENID:
        logger.info(f"[Skip] non-master openid: {user_openid}")
        return

    if content.startswith("approve:"):
        parts = content.split(":")
        if len(parts) >= 3:
            key = parts[1]
            decision = parts[2]
            asyncio.create_task(handle_approval_action(key, decision, user_openid, None, msg_id))
            return

    if content.strip().lower() in ["/new", "/reset", "/清空", "/新对话"]:
        logger.info("[Recv] New session command received")
        await send_to_agy("/new")
        reply = "✅ AGY 会话已重置，开始新对话。"
        await send_message_rest(user_openid, reply)
        return

    if content.strip().lower() in ["/stop", "/停止", "/kill"]:
        logger.info("[Recv] Stop command received")
        await send_to_agy("/new")
        reply = "🛑 已重置 AGY 会话。"
        await send_message_rest(user_openid, reply)
        return

    if _processing:
        await send_message_rest(user_openid, "⚠️ 当前已有任务正在执行中，请稍候。")
        return

    logger.info(f"[QQ -> AGY] {content}")

    _processing = True
    try:
        await send_message_rest(user_openid, "⏳ AGY 正在思考...")
        await send_to_agy(content)
        reply = await wait_for_agy_response()
        if reply == "__APPROVAL_REQUIRED__":
            proc = await asyncio.create_subprocess_exec("tmux", "capture-pane", "-t", TMUX_SESSION, "-p", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            pane_content = clean_ansi(stdout.decode("utf-8", errors="replace"))
            reply = strip_prompt_lines(pane_content)
        if not reply:
            reply = "[AGY 无回复]"
    except Exception as e:
        reply = f"[AGY error] {str(e)[:300]}"
        logger.error(f"Error processing message: {e}")
    finally:
        _processing = False

    logger.info(f"[AGY -> QQ] {reply[:200]}")
    success = await send_message_rest(user_openid, reply)
    if not success:
        logger.error("Reply send failed")


async def handle_approval_action(key: str, decision: str, user_openid: str, group_openid: Optional[str] = None, msg_id: Optional[str] = None) -> bool:
    global _current_approval_key, _processing

    if not _current_approval_key or key != _current_approval_key:
        logger.warning(f"Key mismatch: received {key}, current is {_current_approval_key}")
        feedback = "⚠️ 审批卡片已失效或非最新请求。"
        if group_openid:
            await send_group_message_rest(group_openid, feedback, reply_to=msg_id)
        else:
            await send_message_rest(user_openid, feedback)
        return False

    logger.info(f"Processing approval: decision={decision} for key={key}")

    keystroke = None
    if decision == "allow":
        keystroke = "y"
    elif decision == "allow_similar":
        keystroke = "a"
    elif decision == "allow_always":
        keystroke = "p"
    elif decision == "deny":
        keystroke = "n"

    if not keystroke:
        logger.error(f"Unknown decision type: {decision}")
        return False

    _current_approval_key = None

    _processing = True
    try:
        feedback = f"✅ 已确认操作：[{decision}]，正在提交执行，请稍候..."
        if group_openid:
            await send_group_message_rest(group_openid, feedback, reply_to=msg_id)
        else:
            await send_message_rest(user_openid, feedback)

        await send_to_agy(keystroke)
        reply = await wait_for_agy_response()
        if reply == "__APPROVAL_REQUIRED__":
            reply = "⚠️ AGY 正在等待审批，请在终端确认后重新发送消息。"
        if not reply:
            reply = "[AGY 无回复]"
    except Exception as e:
        reply = f"[AGY error] {str(e)[:300]}"
        logger.error(f"Error handling approval action: {e}")
    finally:
        _processing = False

    if group_openid:
        await send_group_message_rest(group_openid, reply, reply_to=msg_id)
    else:
        await send_message_rest(user_openid, reply)

    return True


async def handle_interaction(d: dict):
    interaction_id = d.get("id")
    if not interaction_id:
        return

    token = await ensure_token()
    client = get_http_client()
    try:
        resp = await client.put(
            f"{API_BASE}/interactions/{interaction_id}",
            headers={
                "Authorization": f"QQBot {token}",
                "Content-Type": "application/json",
                "User-Agent": "AGY-QQ-Bridge/1.0",
            },
            json={"code": 0},
            timeout=5.0,
        )
        logger.info(f"[Interaction] ACK status: {resp.status_code}")
    except Exception as e:
        logger.error(f"[Interaction] ACK exception: {e}")

    author = d.get("author") or {}
    user_openid = d.get("user_openid") or author.get("user_openid")
    if not user_openid:
        user_openid = author.get("member_openid")

    if user_openid != MASTER_OPENID:
        logger.warning(f"[Interaction] Unauthorized click from {user_openid}")
        return

    data_block = d.get("data", {})
    button_data = data_block.get("button_data", "")
    if button_data.startswith("approve:"):
        parts = button_data.split(":")
        if len(parts) >= 3:
            key = parts[1]
            decision = parts[2]
            group_openid = d.get("group_openid")
            msg_id = d.get("id")
            asyncio.create_task(handle_approval_action(key, decision, user_openid, group_openid, msg_id))


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


async def event_loop(ws):
    global _session_id, _last_seq, _running, _ws, heartbeat_task
    _ws = ws
    backoff_idx = 0
    heartbeat_interval = HEARTBEAT_INTERVAL
    heartbeat_task = None

    heartbeat_task = asyncio.create_task(_heartbeat_sender(ws, heartbeat_interval))

    while _running:
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
                        elif t in {"GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"}:
                            task = asyncio.create_task(handle_group_message(d, t))
                            task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                        elif t == "INTERACTION_CREATE":
                            task = asyncio.create_task(handle_interaction(d))
                            task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                        else:
                            logger.debug(f"Unhandled event: {t}")
                        continue

                elif msg.type == 9:
                    logger.warning("WS close received")
                    break

        except Exception as e:
            logger.error(f"Event loop error: {e}")
            if _running:
                backoff = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                logger.info(f"Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff_idx += 1
            else:
                break


async def main():
    global _running
    _running = True

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