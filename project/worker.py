import logging
import hashlib
import os
from multiprocessing import Process, Queue
import random
from socket import socket, AF_INET, SOCK_STREAM


def setup_logging(worker_id):
    worker_logger = logging.getLogger(f"WorkerLogger_{worker_id}")
    file_handler = logging.FileHandler(f"worker_{worker_id}.log")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    worker_logger.addHandler(file_handler)
    return worker_logger


def calculate_md5(path, error_rate):
    temp = setup_logging("calculate")
    temp.info("calculating md5")

    try:
        with open(path, "rb") as f:
            lines = f.readlines()
        file_hash = hashlib.md5(b"".join(lines)).hexdigest()

        # Randomly alter the hash based on the error rate
        if random.random() < error_rate:
            temp.warning("Calculating incorrectly")

            file_hash = "error"
        else:
            temp.info("correct md5")
        return file_hash
    except IOError:
        temp.error("IOError")
        return None


def worker(worker_id, task_queue, worker_errors, worker_warnings):
    # Assign a random error rate
    error_rate = 0.05
    worker_errors[worker_id] = error_rate
    worker_logger = setup_logging(worker_id)
    worker_logger.info(f"Worker {worker_id} started")

    while True:
        print("here")
        path = task_queue.get()  # Get a single file path as a string
        worker_logger.info(f"Worker {worker_id} received task {path}")
        if path == "STOP":
            worker_logger.info(f"Worker {worker_id} stopped")
            break

        # Ensure that the task is a string, not a tuple
        if isinstance(path, tuple):
            path = path[0]  # If it's a tuple, extract the string path

        print(path)

        if os.path.exists(path):  # No need to check if it's a list or string anymore
            try:
                file_hash = calculate_md5(path, error_rate)
                print(file_hash)
                if file_hash is not None:
                    md5_path = path + ".md5"
                    with open(md5_path, "w") as f:
                        f.write(file_hash)
                    worker_logger.info(f"File hash for {path} is {file_hash}")
                else:
                    raise ValueError(f"Failed to calculate MD5 for {path}")
            except Exception as e:
                worker_errors[worker_id] += 1
                worker_warnings[worker_id] += 1
                worker_logger.error(f"Worker error: {e}, Path: {path}")
                worker_logger.info(
                    f"Worker {worker_id} has {worker_errors[worker_id]} errors"
                )
                if worker_warnings[worker_id] > 2:
                    worker_logger.info(
                        f"Worker {worker_id} has received too many warnings. Terminating..."
                    )
                    os._exit(1)
        else:
            worker_logger.warning(f"File {path} not found on the server")


if __name__ == "__main__":
    task_queue = Queue()
    worker_errors = {i: 0 for i in range(1, 6)}
    worker_warnings = {i: 0 for i in range(1, 6)}
    workers = [
        Process(target=worker, args=(i, task_queue, worker_errors, worker_warnings))
        for i in range(1, 6)
    ]
    for worker in workers:
        worker.start()
    for root, _, files in os.walk("./TransactionFiles"):
        for file in files:
            if file.endswith(".json"):
                path = os.path.join(root, file)
                task_queue.put(path)
    for worker in workers:
        worker.join()
