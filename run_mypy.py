import subprocess

def run_mypy():
    subprocess.run(["mypy", "."])
