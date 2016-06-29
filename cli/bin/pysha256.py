import sys
import hashlib
if len(sys.argv) < 2:
    print('Usage: pysha256.py <file-name>')
    sys.exit(1)
BLOCKSIZE = 65536
hasher = hashlib.sha256()
file_name = str(sys.argv[1])
print('Computing sha256 has for file: ' + file_name)
with open(file_name, 'rb') as fd:
    buf = fd.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = fd.read(BLOCKSIZE)
print(hasher.hexdigest())