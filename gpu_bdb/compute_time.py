import pandas as pd
import sys

if len(sys.argv) < 2:
    raise ValueError("result file has to be given as the first argument to the script")

rfile=sys.argv[1]
print("result file:", rfile)

main_df = pd.read_csv("benchmarked_main.csv")
read_df = pd.read_csv("benchmarked_read_tables.csv")
compute_time_df = main_df.elapsed_time_seconds - (read_df.elapsed_time_seconds + read_df.compute_time_seconds)
compute_time = compute_time_df[0]

f = open(rfile, "a")
f.write("%.2f" % compute_time)
f.write("\n")
f.close()

print("%.2f" % compute_time)
