#!/usr/bin/env python3
"""Simulate chat sessions against the Spring AI store.

Each session uses its own conversationId and walks the agent through:
  1. Browse t-shirts, socks, and stickers.
  2. Add between 1 and 10 items to the order.
  3. Ask the agent to place and ship the order.

After every order, optionally fire the server-error, tool-delay and
tool-failure endpoints on configurable cadences (X / Y / Z).

Examples:
  ./simulate-sessions.py --sessions 20
  ./simulate-sessions.py --sessions 50 \\
      --server-error-every 10 \\
      --tool-delay-every 7 \\
      --tool-failure-every 5
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid

PROJECTS = [
    "Spring Boot",
    "Spring AI",
    "Spring Security",
    "Spring Cloud",
    "Spring Data",
    "Spring Batch",
    "Spring for GraphQL",
    "Spring Modulith",
]
TYPES = ["T-Shirt", "Socks", "Sticker"]


def post_json(url, payload, timeout):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        return None, f"URLError: {e.reason}"


def post_empty(url, timeout):
    req = urllib.request.Request(url, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        return None, f"URLError: {e.reason}"


def send_chat(base_url, conversation_id, message, timeout):
    return post_json(
        f"{base_url}/api/chat",
        {"conversationId": conversation_id, "message": message},
        timeout,
    )


def log(prefix, msg):
    print(f"{prefix} {msg}", flush=True)


def run_session(base_url, session_index, timeout):
    conversation_id = str(uuid.uuid4())
    prefix = f"[session {session_index:>4}]"
    log(prefix, f"start conversationId={conversation_id}")

    for category in ("t-shirts", "socks", "stickers"):
        status, _ = send_chat(
            base_url,
            conversation_id,
            f"Please show me all the {category} available in the store.",
            timeout,
        )
        log(prefix, f"browse {category}: HTTP {status}")
        if status is None:
            log(prefix, "aborting session — connection error")
            return

    num_items = random.randint(1, 10)
    order_lines = []
    for _ in range(num_items):
        project = random.choice(PROJECTS)
        item_type = random.choice(TYPES)
        qty = random.randint(1, 3)
        order_lines.append((project, item_type, qty))
    phrase = ", ".join(f"{q} {p} {t}" for p, t, q in order_lines)
    status, _ = send_chat(
        base_url,
        conversation_id,
        f"Add the following items to my order: {phrase}.",
        timeout,
    )
    log(prefix, f"add {num_items} item line(s) [{phrase}]: HTTP {status}")
    if status is None:
        log(prefix, "aborting session — connection error")
        return

    status, _ = send_chat(
        base_url,
        conversation_id,
        "Please place the order now and make sure it is shipped.",
        timeout,
    )
    log(prefix, f"place + ship: HTTP {status}")


def trigger(base_url, endpoint, label, timeout):
    status, _ = post_empty(f"{base_url}{endpoint}", timeout)
    log("[trigger]", f"{label} {endpoint}: HTTP {status}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--base-url", default="http://localhost:8080",
                        help="Store base URL (default: %(default)s)")
    parser.add_argument("--sessions", type=int, default=10,
                        help="Total number of chat sessions to run (default: %(default)s)")
    parser.add_argument("--server-error-every", type=int, default=0, metavar="X",
                        help="Call /api/chat/server-error every X orders (0 disables)")
    parser.add_argument("--tool-failure-every", type=int, default=0, metavar="Y",
                        help="Call /api/chat/shipping-failure every Y orders (0 disables)")
    parser.add_argument("--tool-delay-every", type=int, default=0, metavar="Z",
                        help="Call /api/chat/shipping-delay every Z orders (0 disables)")
    parser.add_argument("--timeout", type=float, default=180.0,
                        help="Per-request timeout in seconds (default: %(default)s)")
    parser.add_argument("--sleep", type=float, default=0.0,
                        help="Seconds to sleep between sessions (default: %(default)s)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed RNG for reproducible runs")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    log("[run]", f"base_url={args.base_url} sessions={args.sessions} "
                 f"server-error-every={args.server_error_every} "
                 f"tool-failure-every={args.tool_failure_every} "
                 f"tool-delay-every={args.tool_delay_every}")

    for i in range(1, args.sessions + 1):
        run_session(args.base_url, i, args.timeout)

        if args.server_error_every and i % args.server_error_every == 0:
            trigger(args.base_url, "/api/chat/server-error", "server-error", args.timeout)
        if args.tool_failure_every and i % args.tool_failure_every == 0:
            trigger(args.base_url, "/api/chat/shipping-failure", "tool-failure", args.timeout)
        if args.tool_delay_every and i % args.tool_delay_every == 0:
            trigger(args.base_url, "/api/chat/shipping-delay", "tool-delay", args.timeout)

        if args.sleep > 0 and i < args.sessions:
            time.sleep(args.sleep)

    log("[run]", "done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        sys.exit(130)
