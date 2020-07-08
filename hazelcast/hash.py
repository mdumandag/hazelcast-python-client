from hazelcast.serialization.bits import FMT_LE_UINT
from hazelcast.six.moves import range


def murmur_hash3_x86_32(data, length):
    """
    murmur3 hash function to determine partition

    :param data: (byte array), input byte array
    :param offset: (long), offset.
    :param size: (long), byte length.
    :param seed: murmur hash seed hazelcast uses 0x01000193
    :return: (int32), calculated hash value.
    """
    nblocks = int(length / 4)

    h1 = 0x01000193

    c1 = 0xcc9e2d51
    c2 = 0x1b873593

    # body
    for block_start in range(0, nblocks * 4, 4):
        # ??? big endian?
        k1 = FMT_LE_UINT.unpack_from(data, block_start + 8)[0]

        k1 = c1 * k1 & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        k1 = (c2 * k1) & 0xFFFFFFFF

        h1 ^= k1
        h1 = (h1 << 13 | h1 >> 19) & 0xFFFFFFFF  # inlined _ROTL32
        h1 = (h1 * 5 + 0xe6546b64) & 0xFFFFFFFF

    # tail
    tail_index = nblocks * 4
    k1 = 0
    tail_size = length & 3

    if tail_size >= 3:
        k1 ^= data[tail_index + 10] << 16
    if tail_size >= 2:
        k1 ^= data[tail_index + 9] << 8
    if tail_size >= 1:
        k1 ^= data[tail_index + 8]

    if tail_size != 0:
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # _ROTL32
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= length
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85ebca6b) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xc2b2ae35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    return -(h1 & 0x80000000) | (h1 & 0x7FFFFFFF)


def hash_to_index(hash, length):
    if hash == 0x80000000:
        return 0
    else:
        return abs(hash) % length
