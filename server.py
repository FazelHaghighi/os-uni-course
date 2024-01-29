import logging
import multiprocessing
from multiprocessing.connection import Listener
from queue import Queue
from threading import Thread
import time

# Worker function module import
from worker import worker as worker_function

# Configure logging
logging.basicConfig(level=logging.INFO)
server_logger = logging.getLogger("ServerLogger")
file_handler = logging.FileHandler("server.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
server_logger.addHandler(file_handler)

# Dictionary to keep track of worker error counts
worker_errors = multiprocessing.Manager().dict({i: 0 for i in range(1, 6)})
worker_warnings = multiprocessing.Manager().dict({i: 0 for i in range(1, 6)})

# Add a new global dictionary to keep track of file to worker mapping
file_to_worker_map = multiprocessing.Manager().dict()


def handle_client(
    conn,
    task_queues,
    file_to_worker_map,
    worker_counter,
    workers,
    worker_errors,
    worker_warnings,
):
    try:
        while True:
            message = conn.recv()
            server_logger.info(f"Received message: {message}")
            print(
                "........................................message......................................"
            )

            # Check if the message is a tuple indicating 'mismatch' or 'calculate_md5'
            if isinstance(message, tuple):
                command, path = message
                print("....command....: ", command)
                print("....path....: ", path)
                if command == "mismatch":
                    print(
                        "........................................mismatch......................................"
                    )
                    server_logger.info(f"Received mismatch for: {path}")
                    worker_id = identify_worker_for_path(path, file_to_worker_map)
                    print(".....worker id......: ", worker_id)
                    if worker_id is not None:
                        worker_warnings[worker_id] += 1
                        server_logger.warning(
                            f"Worker {worker_id} MD5 mismatch for file {path}"
                        )
                        print(
                            ".....worker_id.....: ",
                            worker_id,
                            ".....worker_warnings[worker_id].....: ",
                            worker_warnings[worker_id],
                        )
                        if worker_warnings[worker_id] > 2:
                            replace_worker(
                                worker_id - 1,
                                workers,
                                task_queues,
                                worker_function,
                                worker_errors,
                                worker_warnings,
                            )
                    print(
                        "........................................calculate_md5......................................"
                    )
                    server_logger.info(f"Received request to calculate MD5 for: {path}")
                    # Round-robin distribution of file batches to workers using a counter
                    worker_id = (
                        worker_counter.value % len(task_queues)
                    ) + 1  # Worker IDs start from 1
                    task_queue = task_queues[
                        worker_id - 1
                    ]  # Adjust for zero-based indexing

                    if (
                        path in file_to_worker_map
                        and file_to_worker_map[path]["command"] == "mismatch"
                    ):
                        continue

                    # Update the mapping of file to worker
                    file_to_worker_map[path] = {
                        "worker_id": worker_id,
                        "command": command,
                    }
                    # print(".....file_to_worker_map.....: ", file_to_worker_map)
                    print(".....worker_id.....: ", worker_id)

                    # Put the path in the worker's task queue
                    task_queue.put(path)

                    # Increment the counter
                    with worker_counter.get_lock():  # Synchronize access to the counter
                        worker_counter.value += 1

                    server_logger.info(f"Assigned {path} to worker {worker_id}")

                elif command == "calculate_md5":
                    print(
                        "........................................calculate_md5......................................"
                    )
                    server_logger.info(f"Received request to calculate MD5 for: {path}")
                    # Round-robin distribution of file batches to workers using a counter
                    worker_id = (
                        worker_counter.value % len(task_queues)
                    ) + 1  # Worker IDs start from 1
                    task_queue = task_queues[
                        worker_id - 1
                    ]  # Adjust for zero-based indexing

                    # Update the mapping of file to worker
                    file_to_worker_map[path] = {
                        "worker_id": worker_id,
                        "command": command,
                    }
                    # print(".....file_to_worker_map.....: ", file_to_worker_map)
                    print(".....worker_id.....: ", worker_id)

                    # Put the path in the worker's task queue
                    task_queue.put(path)

                    # Increment the counter
                    with worker_counter.get_lock():  # Synchronize access to the counter
                        worker_counter.value += 1

                    server_logger.info(f"Assigned {path} to worker {worker_id}")

                elif command == "-":
                    print("done")
                    server_logger.info(f"Received done for: {path}")
            else:
                server_logger.error(f"Unknown message type: {message}")

    except EOFError:
        server_logger.info("Client has disconnected")
    finally:
        conn.close()


# Helper function to replace a worker
def replace_worker(
    worker_index, workers, task_queues, worker_function, worker_errors, worker_warnings
):
    server_logger.error(
        f"Worker {worker_index + 1} terminated due to too many warnings. Replacing..."
    )
    workers[worker_index].terminate()
    workers[worker_index].join()  # Wait for the worker to finish
    new_worker = multiprocessing.Process(
        target=worker_function,
        args=(
            worker_index + 1,
            task_queues[worker_index],
            worker_errors,
            worker_warnings,
        ),
    )
    new_worker.start()
    workers[worker_index] = new_worker
    worker_warnings[worker_index + 1] = 0  # Reset the warning counter


def identify_worker_for_path(path, file_to_worker_map):
    # Retrieve the worker ID for the given file path
    worker_id = file_to_worker_map.get(path).get("worker_id")
    if worker_id is not None:
        return worker_id
    else:
        # If the path is not found, it means the file was not assigned yet or the worker ID is unknown
        server_logger.error(f"Could not identify the worker for path: {path}")
        return None


def start_server(address):
    # Manager for shared resources among processes
    manager = multiprocessing.Manager()
    worker_errors = manager.dict({i: 0 for i in range(1, 6)})
    worker_warnings = manager.dict({i: 0 for i in range(1, 6)})
    file_to_worker_map = manager.dict()
    print(".....file_to_worker_map.....: ", file_to_worker_map)

    # Queue for task distribution
    task_queues = [manager.Queue() for _ in range(5)]

    # Start worker processes
    workers = [
        multiprocessing.Process(
            target=worker_function,
            args=(i + 1, task_queues[i], worker_errors, worker_warnings),
        )
        for i in range(5)
    ]
    for worker in workers:
        worker.start()

    # Function to monitor and replace workers
    def monitor_workers():
        while True:
            for i, worker in enumerate(workers):
                if not worker.is_alive():
                    server_logger.error(
                        f"Worker {i+1} terminated unexpectedly. Restarting..."
                    )
                    worker_warnings[i + 1] += 1
                    if worker_warnings[i + 1] > 2:
                        server_logger.error(
                            f"Worker {i + 1} has received too many warnings. Terminating..."
                        )
                    else:
                        server_logger.info(f"Restarting worker {i + 1}...")
                    replace_worker(
                        i,
                        workers,
                        task_queues,
                        worker_function,
                        worker_errors,
                        worker_warnings,
                    )
            time.sleep(10)  # Check every 10 seconds

    # Start the monitoring thread
    monitoring_thread = Thread(target=monitor_workers)
    monitoring_thread.start()

    worker_counter = multiprocessing.Value(
        "i", 0
    )  # Shared counter for worker round-robin

    # Server listener
    with Listener(address) as listener:
        server_logger.info("Server started and listening for connections")
        while True:
            conn = listener.accept()
            server_logger.info("Connection accepted")
            message = conn.recv()
            server_logger.info(f"Received message: {message}")
            print(
                "........................................message......................................"
            )
            if message == "client":
                print(
                    "........................................client......................................"
                )
                server_logger.info("Client connected")
                Thread(
                    target=handle_client,
                    args=(
                        conn,
                        task_queues,
                        file_to_worker_map,
                        worker_counter,
                        workers,
                        worker_errors,
                        worker_warnings,
                    ),
                ).start()
            elif message == "worker":
                server_logger.info("Worker connected")
                # Handle worker connection if necessary
                print(
                    "........................................worker......................................"
                )
                task_queues.put(conn)
                server_logger.info("Worker connected")
            else:
                server_logger.error(f"Unknown message: {message}")


if __name__ == "__main__":
    start_server(("localhost", 6001))
