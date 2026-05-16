import asyncio
import json
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional


Body = Dict[str, Any]
Handler = Callable[["Request"], Awaitable[Optional[Body]]]


@dataclass
class Request:
    """
    A single Maelstrom message.

    Every Maelstrom message has:
      - src: sender node/client
      - dest: destination node
      - body: message payload

    Example body:
        {
            "type": "echo",
            "msg_id": 1,
            "echo": "hello"
        }
    """

    src: str
    dest: str
    body: Body


class Node:
    """
    Minimal Maelstrom node helper.

    This is intentionally close to the Go API style:

        node = Node()

        @node.handler
        async def echo(req):
            return {
                "type": "echo_ok",
                "echo": req.body["echo"],
            }

        node.run()

    Handler names are message types.

    So this:

        async def echo(req): ...

    handles messages with:

        {"type": "echo", ...}
    """

    def __init__(self):
        self.node_id: Optional[str] = None
        self.node_ids: list[str] = []
        self.pending: Dict[int, asyncio.Future[Body]] = {}
        self.next_msg_id = 0
        self.handlers: Dict[str, Handler] = {}

        # Stores periodic background tasks registered with node.every().
        # Each entry is:
        #   (interval_seconds, async_function)
        self.periodic_tasks = []

        @self.handler
        async def init(req: Request) -> Body:
            """
            Built-in init handler.

            Maelstrom sends init first so the node learns:
              - its own node_id
              - the full list of node_ids in the cluster
            """
            self.node_id = req.body["node_id"]
            self.node_ids = req.body["node_ids"]
            return {"type": "init_ok"}

    def every(self, seconds, func):
        """
        Register a periodic async task.

        Example:

            async def gossip():
                ...

            node.every(1, gossip)

        The task will start automatically when node.run() starts.
        """
        self.periodic_tasks.append((seconds, func))

    def handler(self, func: Handler) -> Handler:
        """
        Register a handler for a message type.

        The Python function name is used as the Maelstrom message type.

        Example:

            @node.handler
            async def echo(req):
                ...

        registers a handler for body["type"] == "echo".
        """
        self.handlers[func.__name__] = func
        return func

    async def reply(self, req: Request, body: Body):
        """
        Reply to a request.

        This automatically:
          - sets src to this node
          - sets dest to the original sender
          - adds a fresh msg_id
          - adds in_reply_to pointing at the request msg_id
        """
        body["msg_id"] = self.next_msg_id
        self.next_msg_id += 1

        body["in_reply_to"] = req.body["msg_id"]

        msg = {
            "src": self.node_id,
            "dest": req.src,
            "body": body,
        }

        self._write(msg)

    async def rpc(self, dest: str, body: Body) -> Body:
        msg_id = self.next_msg_id
        self.next_msg_id += 1

        body["msg_id"] = msg_id

        fut = asyncio.get_running_loop().create_future()
        self.pending[msg_id] = fut

        msg = {
            "src": self.node_id,
            "dest": dest,
            "body": body,
        }

        self._write(msg)

        return await fut

    async def send(self, dest: str, body: Body):
        """
        Send a normal message.

        This is not a reply. Use this for node-to-node messages.

        Example:

            await node.send("n2", {"type": "broadcast", "message": 123})
        """
        body["msg_id"] = self.next_msg_id
        self.next_msg_id += 1

        msg = {
            "src": self.node_id,
            "dest": dest,
            "body": body,
        }

        self._write(msg)

    async def _handle_msg(self, line: str):
        """
        Parse one incoming JSON line and dispatch it to the right handler.
        """
        try:
            msg = json.loads(line)

            req = Request(
                src=msg["src"],
                dest=msg["dest"],
                body=msg["body"],
            )

            in_reply_to = req.body.get("in_reply_to")
            if in_reply_to is not None:
                fut = self.pending.pop(in_reply_to, None)
                if fut is not None and not fut.done():
                    fut.set_result(req.body)
                return

            msg_type = req.body["type"]
            handler = self.handlers.get(msg_type)

            if handler is None:
                return

            res_body = await handler(req)

            if res_body is not None:
                await self.reply(req, res_body)

        except Exception:
            # traceback.print_exc() gives full stack traces to stderr,
            # which is safe because Maelstrom only reads stdout.
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()

    def _write(self, msg: Body):
        """
        Write one JSON message to stdout.

        Maelstrom communicates with nodes using newline-delimited JSON.
        Logs should go to stderr, never stdout.
        """
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()

    def run(self):
        """
        Start the node event loop.

        Reads JSON messages from stdin forever.
        Each message is handled in its own asyncio task.
        """

        async def main():
            loop = asyncio.get_running_loop()

            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)

            await loop.connect_read_pipe(lambda: protocol, sys.stdin)

            periodic_task_handles = []

            for seconds, func in self.periodic_tasks:

                async def periodic_loop(seconds=seconds, func=func):
                    while True:
                        await asyncio.sleep(seconds)
                        await func()

                periodic_task_handles.append(asyncio.create_task(periodic_loop()))

            try:
                while True:
                    line = await reader.readline()

                    if not line:
                        break

                    asyncio.create_task(self._handle_msg(line.decode()))
            finally:
                for task in periodic_task_handles:
                    task.cancel()

                await asyncio.gather(
                    *periodic_task_handles,
                    return_exceptions=True,
                )

        asyncio.run(main())

    def log(self, *args):
        """
        Write debug logs to stderr.

        Never log to stdout because stdout is reserved for the
        Maelstrom JSON protocol.
        """
        print(*args, file=sys.stderr, flush=True)