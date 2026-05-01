HASH_METHOD = 'blake3'

# 256chunk/group * (32KiB/chunk * 1.2) == ~10MiB/group  -->  ~200 groups for a 2GB file
CHUNK_GROUP_AVG_SIZE = 256
CHUNK_GROUP_MIN_SIZE = 64
CHUNK_GROUP_MAX_SIZE = 1024
