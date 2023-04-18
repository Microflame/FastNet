import sys
import subprocess

import matplotlib.pyplot as plt

BIN = sys.argv[1]

if '/' not in BIN:
    BIN = './' + BIN

# sizes = [8, 32, 128]
sizes = [8, 32, 128, 512, 2 * 1024, 8 * 1024, 32 * 1024, 128 * 1024, 512 * 1024, 2 * 1024 * 1024]
tps = []

for size in sizes:
    res = subprocess.getoutput(f'{BIN} {size}')
    throughput = float(res.split()[1])
    tps.append(throughput)

plt.scatter(sizes, tps)
plt.xscale('log')
plt.xlabel('recv size, bytes')
plt.ylabel('Throughput, MB/s')
plt.grid(True)

plt.savefig('recv_perf.png')
