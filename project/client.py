import logging
import os
import hashlib
import queue
import threading
from multiprocessing.connection import Client
from time import sleep

log_lock = threading.Lock()  # Add a lock for logging
request_lock = threading.Lock()

# Configure logging
client_logger = logging.getLogger("ClientLogger")
file_handler = logging.FileHandler("client.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
client_logger.addHandler(file_handler)
client_logger.setLevel(logging.INFO)  # Set the logging level
client_logger.propagate = (
    False  # Prevent the log messages from being propagated to the root logger
)


# Calculate MD5 hash function
def calculate_md5(path):
    with open(path, "rb") as f:
        lines = f.readlines()
    file_hash = hashlib.md5(b"".join(lines))

    return file_hash.hexdigest()


# Compare MD5 hash function
def compare_md5(original_path, md5_path):
    original_hash = calculate_md5(original_path)
    with open(md5_path, "r") as f:
        md5_hash = f.read().strip()
        print("md5_hash: ", md5_hash, "original_hash: ", original_hash)
    return original_hash == md5_hash


# Send file paths to the server
def send_file_paths(conn, file_paths):
    for path in file_paths:
        md5_path = path + ".md5"
        if os.path.exists(md5_path) and not compare_md5(path, md5_path):
            print_alert(f"Alert: MD5 mismatch for {path}")

        try:
            with request_lock:
                conn.send(path)
                client_logger.info(f"Sent path: {path} to server")
        except Exception as e:
            client_logger.error(f"Error sending path: {path} to server: {e}")


def print_alert(message):
    # Use client_logger to log the message
    client_logger.warning(message)
    print(message)


def examine_file(file_queue: queue.Queue, server_address):
    with Client(server_address) as conn:
        conn.send("client")
        client_logger.info("Connected to the server")
        while True:
            client_logger.info(
                "queue with length: %s",
                file_queue.qsize(),
            )
            print("queue with length: %s", file_queue.qsize())

            path = file_queue.get()
            client_logger.info(f"Got path: {path} from queue")
            # if path is None:  # None is used as a signal to stop the thread
            #     break
            md5_path = path + ".md5"
            try:
                if not os.path.exists(md5_path):
                    print(
                        "............................calculate_md5................................"
                    )
                    # Request server to calculate MD5 hash
                    with request_lock:
                        conn.send(("calculate_md5", path))
                        client_logger.info(f"Sent request for {path} to the server")
                        file_queue.put(path)
                        client_logger.info(f"Put {path} back to the queue")
                else:
                    client_logger.info("calculate_md5")
                    # Compare MD5 hashes
                    md5_match = compare_md5(path, md5_path)
                    with request_lock:
                        if not md5_match:
                            print(
                                "............................mismatch................................"
                            )
                            print_alert(f"Alert: MD5 mismatch for {path}")
                            # Send a signal to the server about the mismatch
                            conn.send(("mismatch", path))
                            client_logger.error("mismatch")
                            file_queue.put(path)

                        else:
                            client_logger.info(f"success for path: {path}")
                            # If MD5 matches, send a message to update file_to_worker_map
                            conn.send(("-", path))
            except Exception as e:
                print_alert(f"Error processing file {path}: {e}")


# Modified find_files_and_process to only find files
def find_files(root_dir, file_extension, file_queue):
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(file_extension):
                file_queue.put(os.path.join(root, file))


if __name__ == "__main__":
    file_queue = queue.Queue()
    server_address = ("localhost", 6001)
    root_dir = "./TransactionFiles"
    file_extension = ".json"

    # Start file finding thread
    file_finding_thread = threading.Thread(
        target=find_files, args=(root_dir, file_extension, file_queue)
    )
    file_finding_thread.start()

    # Start file examining threads
    examining_threads = []
    for _ in range(5):
        t = threading.Thread(target=examine_file, args=(file_queue, server_address))
        t.start()
        examining_threads.append(t)

    # Wait for file finding thread to finish
    file_finding_thread.join()

    # # Signal the examining threads to stop
    # for _ in examining_threads:
    #     file_queue.put(None)

    # Wait for all examining threads to finish
    for t in examining_threads:
        t.join()
