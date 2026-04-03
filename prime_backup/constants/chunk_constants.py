HASH_METHOD = 'blake3'

CDC_AVG_SIZE = 32 * 1024          # 32KiB
CDC_MIN_SIZE = CDC_AVG_SIZE // 4  # 8KiB
CDC_MAX_SIZE = CDC_AVG_SIZE * 8   # 256KiB

# 256chunk/group * (32KiB/chunk * 1.2) == ~10MiB/group  -->  ~200 groups for a 2GB file
CHUNK_GROUP_AVG_SIZE = 256
CHUNK_GROUP_MIN_SIZE = 64
CHUNK_GROUP_MAX_SIZE = 1024
