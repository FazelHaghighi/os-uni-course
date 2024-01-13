from os import path, walk, remove

folder_path = "./TransactionFiles"
files_addresses = []
for root, dirs, files in walk(folder_path):
    for file in files:
        if file.endswith(".json.md5"):
            files_addresses.append(path.join(root, file))

for adr in files_addresses:
    remove(adr)
