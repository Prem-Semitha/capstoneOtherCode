import os
import multiprocessing

# Using os
print("Number of logical processors (os):", os.cpu_count())

# Using multiprocessing
print("Number of logical processors (multiprocessing):", multiprocessing.cpu_count())

