import numpy as np
import matplotlib.pyplot as plt

# Load data from process files
data = []
for rank in range(1, 25):
    filename = f"rank_{rank}_output.txt"
    data.append(np.loadtxt(filename))

# Perform data analysis and visualization as needed
# For example, you can calculate statistics or create plots
for rank, reading_times in enumerate(data):
    plt.plot(reading_times, label=f"Process {rank}")

plt.xlabel("Time Window")
plt.ylabel("Reading Time")
plt.legend()
plt.show()
