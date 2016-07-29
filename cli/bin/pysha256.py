import sys
import hashlib
if len(sys.argv) < 2:
    print('Usage: pysha256.py <file-name1> [file-name2] ...')
    sys.exit(1)
BLOCKSIZE = 65536
for file_name in sys.argv[1:]:
    hasher = hashlib.sha256()
    with open(file_name, 'rb') as fd:
        buf = fd.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = fd.read(BLOCKSIZE)
    print('%s  %s' % (hasher.hexdigest(), file_name))
