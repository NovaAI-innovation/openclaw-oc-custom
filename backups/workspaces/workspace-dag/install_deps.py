import subprocess
import sys

def install():
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3", "networkx"])
    except Exception as e:
        print(f"Failed to install dependencies: {e}")

install()
