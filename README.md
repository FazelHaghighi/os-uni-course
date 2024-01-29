# Operating Systems Course Project — Server, Client, Worker Architecture

## How to Run

### Running the Server

Please ensure Python is installed on your system before attempting to run the server.

1. Open terminal in the directory where the OS-course project folder is located.
2. Execute the run command:

```powershell
python server.py
```

### Running the Client

1. Open another terminal for the client.
2. Execute the client command:

```powershell
python client.py
```

## Project Description

This project is designed to manage a set of server, client, and worker processes to maintain file integrity across university servers.

### Step One

**Objective**: Implement a system that verifies file integrity to detect any unintended alterations.

**Process Implementation**:
- **Server Process**: Initiates one server process and five worker processes.
  - Server creates a new worker if one exits due to an error.
  - Server awaits connections from client and worker processes.
- **Client Process**:
  - Upon connection, clients send the server several file addresses.
- **Worker Process**:
  - Connects to the server to receive file addresses.
  - Assigned a maximum of five file addresses per request.
  - Workers calculate the MD5 checksums of received files and save them with a `.md5` extension.
  - After processing, workers seek new files to process.
- **Communication**: Utilises network sockets or pipes for process communication.
- **MD5 Calculation**: Can use available library code or internet snippets.
- **Testing**: Employ client processes to gradually supply the server with a substantial number of file addresses.

### Step Two — Expanding the Client Program

**First Part**:
- The client program now runs six concurrent threads.
  - One thread performs a recursive system file listing.
  - Five threads validate the listed files, each responsible for a unique file to prevent overlap.
  - Checker threads request file addresses, with synchronization to avoid race conditions when contacting the server.

**Second Part** — Identifying Malicious Workers:
- Workers with a file MD5 mismatch incur penalties.
- Workers accumulating over two warnings are terminated.
- The server must always maintain five operational workers, replacing as necessary.
- Log all activity, particularly penalties and worker terminations.
- Automate the testing process.