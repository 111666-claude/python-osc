"""Small example of the bounded-queue asynchronous OSC TCP client.

A burst of messages is produced from several coroutines while a slow receiver
drains them. The bounded queue applies backpressure instead of accumulating
unbounded write tasks, and a single background writer keeps every OSC frame
ordered and intact.

Run alongside ``async_tcp_server.py`` (or any OSC TCP server).
"""

import argparse
import asyncio

from pythonosc import tcp_client
from pythonosc.send_queue import SendQueueFull, SendQueueFullPolicy


async def burst(
    client: tcp_client.QueuedAsyncSimpleTCPClient, producer_id: int
) -> None:
    for i in range(10):
        try:
            sent = await client.send_message(
                "/burst",
                [producer_id, i],
                full_policy=SendQueueFullPolicy.ERROR,
            )
            print(f"producer {producer_id}: message {i} sent={sent}")
        except SendQueueFull:
            # The receiver cannot keep up; fall back to waiting for capacity.
            await client.send_message("/burst", [producer_id, i])
        await asyncio.sleep(0.01)


async def init_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="127.0.0.1", help="The ip of the OSC server")
    parser.add_argument(
        "--port", type=int, default=5005, help="The port the OSC server is listening on"
    )
    parser.add_argument(
        "--mode",
        default="1.1",
        help="The OSC protocol version of the server (default is 1.1)",
    )
    args = parser.parse_args()

    async with tcp_client.QueuedAsyncSimpleTCPClient(
        args.ip,
        args.port,
        mode=args.mode,
        max_messages=25,
        max_bytes=1_000_000,
        full_policy=SendQueueFullPolicy.WAIT,
    ) as client:
        async with asyncio.TaskGroup() as tg:
            for producer_id in range(4):
                tg.create_task(burst(client, producer_id))
    # Exiting the context manager drains (flushes) all queued messages.
    # Use ``await client.close(drain=False)`` to discard them instead.


if __name__ == "__main__":
    asyncio.run(init_main())
