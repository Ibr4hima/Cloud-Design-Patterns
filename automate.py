import subprocess
import sys

def main():
    # Run main.py
    print("\nStarting cluster setup with main.py...")
    result = subprocess.run([sys.executable, 'main.py'])
    if result.returncode != 0:
        print("Cluster setup failed!")
        sys.exit(1)

    # Run benchmark.py
    print("\nStarting benchmarking with benchmark.py...")
    result = subprocess.run([sys.executable, 'benchmark.py'])
    if result.returncode != 0:
        print("Benchmarking failed!")
        sys.exit(1)

    print("\nAll operations completed successfully!")

if __name__ == "__main__":
    main()