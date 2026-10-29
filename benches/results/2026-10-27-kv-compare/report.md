# kv-compare results

## Basic operations

### Read one key

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 756k | 23.7k |
| LMDB | 844k | 24.2k |
| fjall | 240k | 18.4k |
| RocksDB | 344k | 10.2k |
| redb | 542k | 19.5k |
| SQLite | 290k | 20.8k |
| ZeroDB, trusted file | 870k | 24.0k |

### Read 100 keys in order

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 234k | 4.3k |
| LMDB | 280k | 4.4k |
| fjall | 31.5k | 3.6k |
| RocksDB | 43.8k | 5.9k |
| redb | 139k | 3.7k |
| SQLite | 64.8k | 4.7k |
| ZeroDB, trusted file | 300k | 4.3k |

### Write one key, saved to disk

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 5.2k | 4.5k |
| LMDB | 3.8k | 3.4k |
| fjall | 15.2k | 13.7k |
| RocksDB | 11.2k | 7.9k |
| redb | 3.8k | 3.4k |
| SQLite | 16.2k | 11.4k |
| ZeroDB, trusted file | 5.2k | 4.5k |

### Write 1,000 keys per commit

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 64.0k | 14.4k |
| LMDB | 75.2k | 14.2k |
| fjall | 280k | 266k |
| RocksDB | 504k | 404k |
| redb | 64.7k | 12.6k |
| SQLite | 27.4k | 17.6k |
| ZeroDB, trusted file | 71.8k | 14.1k |

### Load the data set

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 141k | 21.0k |
| LMDB | 176k | 25.1k |
| fjall | 409k | 272k |
| RocksDB | 600k | 475k |
| redb | 146k | 30.0k |
| SQLite | 18.5k | 16.0k |
| ZeroDB, trusted file | 135k | 21.3k |

### Reads on more threads: fits in memory

| engine | 1 thr | 2 thr | 4 thr | 8 thr | 16 thr |
|---|---:|---:|---:|---:|---:|
| ZeroDB | 756k | 1.3M | 2.4M | 2.4M | 2.3M |
| LMDB | 844k | 1.4M | 2.5M | 2.8M | 2.7M |
| fjall | 240k | 456k | 892k | 872k | 879k |
| RocksDB | 344k | 676k | 1.4M | 1.4M | 1.3M |
| redb | 542k | 815k | 1.2M | 1.2M | 1.2M |
| SQLite | 290k | 421k | 851k | 820k | 771k |
| ZeroDB, trusted file | 870k | 1.4M | 2.7M | 2.6M | 2.7M |

### Reads on more threads: larger than memory

| engine | 1 thr | 2 thr | 4 thr | 8 thr | 16 thr |
|---|---:|---:|---:|---:|---:|
| ZeroDB | 23.7k | 47.1k | 91.0k | 122k | 122k |
| LMDB | 24.2k | 48.0k | 93.1k | 123k | 123k |
| fjall | 18.4k | 37.4k | 74.3k | 89.7k | 89.6k |
| RocksDB | 10.2k | 34.6k | 79.4k | 128k | 128k |
| redb | 19.5k | 38.5k | 75.6k | 103k | 103k |
| SQLite | 20.8k | 37.0k | 75.2k | 110k | 110k |
| ZeroDB, trusted file | 24.0k | 47.6k | 92.3k | 122k | 122k |

### Reads while one thread writes: fits in memory

| engine | 1 thr | 2 thr | 4 thr | 8 thr | 16 thr |
|---|---:|---:|---:|---:|---:|
| ZeroDB | 655k | 1.2M | 1.9M | 1.9M | 2.0M |
| LMDB | 738k | 1.4M | 2.0M | 2.4M | 2.4M |
| fjall | 101k | 223k | 295k | 336k | 449k |
| RocksDB | 135k | 285k | 363k | 408k | 502k |
| redb | 269k | 356k | 564k | 612k | 1.0M |
| SQLite | 133k | 224k | 319k | 411k | 408k |
| ZeroDB, trusted file | 769k | 1.3M | 2.2M | 2.1M | 2.2M |

Read p99 µs / writes per second at each thread count:

| engine | 1 thr | 2 thr | 4 thr | 8 thr | 16 thr |
|---|---:|---:|---:|---:|---:|
| ZeroDB | 2.1 / 20.9k | 2.4 / 21.1k | 3.3 / 18.1k | 3.9 / 19.4k | 3.8 / 15.0k |
| LMDB | 1.7 / 20.4k | 1.8 / 21.1k | 2.9 / 19.1k | 2.7 / 14.5k | 2.6 / 10.0k |
| fjall | 15.0 / 238k | 14.5 / 232k | 16.0 / 236k | 16.3 / 228k | 16.6 / 184k |
| RocksDB | 14.7 / 302k | 14.2 / 297k | 15.2 / 266k | 16.0 / 214k | 16.3 / 126k |
| redb | 12.9 / 31.7k | 82.9 / 28.9k | 82.9 / 19.1k | 182.3 / 9.2k | 36.4 / 4.3k |
| SQLite | 26.9 / 27.2k | 25.3 / 27.2k | 109.6 / 26.3k | 356.4 / 11.6k | 745.5 / 7.9k |
| ZeroDB, trusted file | 2.0 / 22.6k | 2.0 / 22.2k | 2.9 / 18.2k | 3.9 / 18.1k | 3.5 / 14.0k |

### Reads while one thread writes: larger than memory

| engine | 1 thr | 2 thr | 4 thr | 8 thr | 16 thr |
|---|---:|---:|---:|---:|---:|
| ZeroDB | 14.7k | 33.7k | 79.4k | 111k | 115k |
| LMDB | 12.0k | 33.1k | 76.2k | 111k | 115k |
| fjall | 6.3k | 12.9k | 23.5k | 38.7k | 51.4k |
| RocksDB | 7.4k | 11.2k | 22.5k | 47.0k | 57.7k |
| redb | 17.0k | 28.8k | 53.8k | 75.4k | 76.6k |
| SQLite | 19.3k | 42.1k | 86.7k | 136k | 141k |
| ZeroDB, trusted file | 13.7k | 35.5k | 79.6k | 110k | 114k |

Read p99 µs / writes per second at each thread count:

| engine | 1 thr | 2 thr | 4 thr | 8 thr | 16 thr |
|---|---:|---:|---:|---:|---:|
| ZeroDB | 778.2 / 8.7k | 471.0 / 11.2k | 186.4 / 13.7k | 202.8 / 10.7k | 323.6 / 6.2k |
| LMDB | 942.1 / 6.5k | 479.2 / 10.5k | 202.8 / 13.3k | 202.8 / 11.1k | 315.4 / 6.3k |
| fjall | 462.9 / 196k | 565.2 / 194k | 1040.4 / 209k | 1294.3 / 192k | 1720.3 / 192k |
| RocksDB | 729.1 / 297k | 991.2 / 295k | 1007.6 / 295k | 1065.0 / 260k | 1425.4 / 264k |
| redb | 141.3 / 11.1k | 161.8 / 9.9k | 194.6 / 8.8k | 231.4 / 6.4k | 405.5 / 3.8k |
| SQLite | 182.3 / 15.4k | 178.2 / 11.9k | 170.0 / 3.7k | 215.0 / 2.8k | 348.2 / 2.7k |
| ZeroDB, trusted file | 942.1 / 8.4k | 219.1 / 12.2k | 186.4 / 13.7k | 194.6 / 11.0k | 331.8 / 6.3k |

## Cost

### Disk space after load

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 1.9 GB | 19.0 GB |
| LMDB | 2.0 GB | 19.0 GB |
| fjall | 1.7 GB | 13.2 GB |
| RocksDB | 1.8 GB | 15.9 GB |
| redb | 1.9 GB | 18.2 GB |
| SQLite | 1.5 GB | 14.1 GB |
| ZeroDB, trusted file | 1.9 GB | 19.0 GB |

### Write amplification (random overwrites)

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 40.5× | 108.4× |
| LMDB | 35.3× | 106.9× |
| fjall | 9.4× | 10.2× |
| RocksDB | 5.2× | 6.5× |
| redb | 20.2× | 47.2× |
| SQLite | 96.3× | 92.0× |
| ZeroDB, trusted file | 37.5× | 107.2× |

### Peak memory during single-thread reads (memory-mapped engines count cached file pages)

| engine | Fits in memory | Larger than memory |
|---|---:|---:|
| ZeroDB | 2.0 GB | 8.5 GB |
| LMDB | 2.0 GB | 8.5 GB |
| fjall | 1.3 GB | 2.3 GB |
| RocksDB | 2.1 GB | 2.5 GB |
| redb | 1.8 GB | 2.2 GB |
| SQLite | 1.5 GB | 2.2 GB |
| ZeroDB, trusted file | 2.0 GB | 8.5 GB |

## Stability

| engine | records/s overall | slowest 10 s | fastest 10 s |
|---|---:|---:|---:|
| ZeroDB | 35.2k | 23.4k | 69.3k |
| LMDB | 35.8k | 23.7k | 84.3k |
| fjall | 187k | 0 | 374k |
| RocksDB | 251k | 9.8k | 552k |
| redb | 25.5k | 15.5k | 70.0k |
| SQLite | 16.2k | 16.1k | 18.2k |
| ZeroDB, trusted file | 35.6k | 18.4k | 72.5k |

