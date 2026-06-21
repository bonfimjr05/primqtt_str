import sys
import time

for linha in sys.stdin:
    ts = time.time()
    sys.stdout.write(f"{ts:.6f} {linha}")
    sys.stdout.flush()