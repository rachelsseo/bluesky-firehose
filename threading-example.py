import queue
import threading

q = queue.Queue(maxsize=10000)  # Limit memory usage

def on_message(ws, message):
    q.put(message)  # Takes microseconds

def worker():
    while True:
        message = q.get()
        # Do slow processing here
        process(message)

# Start worker threads
for _ in range(4):  # 4 workers
    threading.Thread(target=worker, daemon=True).start()

ws.run_forever()