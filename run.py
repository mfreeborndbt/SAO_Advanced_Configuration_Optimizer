#!/usr/bin/env python3
"""
SAO Advanced Configuration Optimizer — cross-platform launcher.

Works on macOS, Linux, and Windows. Requires Python 3.8+.

Usage:
    python run.py            # default port 5555
    python run.py --port 8080
"""

import os
import platform
import subprocess
import sys
import threading
import time
import webbrowser

MIN_PYTHON = (3, 8)
DEFAULT_PORT = 5555
VENV_DIR = ".venv"
REQUIREMENTS = "requirements.txt"

ROOT = os.path.dirname(os.path.abspath(__file__))


def check_python_version():
    if sys.version_info < MIN_PYTHON:
        sys.exit(
            f"Error: Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ is required. "
            f"You have {platform.python_version()}.\n"
            f"Download from https://python.org"
        )


def venv_python():
    """Return the path to the Python binary inside the venv."""
    if platform.system() == "Windows":
        return os.path.join(ROOT, VENV_DIR, "Scripts", "python.exe")
    return os.path.join(ROOT, VENV_DIR, "bin", "python")


def ensure_venv():
    """Create a virtual environment if one doesn't exist."""
    py = venv_python()
    if os.path.isfile(py):
        return py

    print("Creating virtual environment...")
    subprocess.check_call([sys.executable, "-m", "venv", os.path.join(ROOT, VENV_DIR)])
    if not os.path.isfile(py):
        sys.exit(f"Error: Failed to create virtual environment at {VENV_DIR}/")
    return py


def install_deps(py):
    """Install/upgrade dependencies from requirements.txt."""
    req_path = os.path.join(ROOT, REQUIREMENTS)
    if not os.path.isfile(req_path):
        sys.exit(f"Error: {REQUIREMENTS} not found in {ROOT}")

    print("Installing dependencies...")
    subprocess.check_call(
        [py, "-m", "pip", "install", "-q", "--upgrade", "pip"],
        stdout=subprocess.DEVNULL,
    )
    subprocess.check_call(
        [py, "-m", "pip", "install", "-q", "-r", req_path],
    )


def open_browser(port):
    """Open the browser after a short delay to let the server start."""
    time.sleep(2)
    webbrowser.open(f"http://localhost:{port}")


def main():
    os.chdir(ROOT)
    check_python_version()

    # Parse --port argument
    port = DEFAULT_PORT
    args = sys.argv[1:]
    if "--port" in args:
        idx = args.index("--port")
        if idx + 1 < len(args):
            try:
                port = int(args[idx + 1])
            except ValueError:
                sys.exit(f"Error: Invalid port number '{args[idx + 1]}'")

    py = ensure_venv()
    install_deps(py)

    url = f"http://localhost:{port}"
    print()
    print("  SAO Advanced Configuration Optimizer")
    print("  ------------------------------------")
    print(f"  Running at: {url}")
    print("  Stop with:  Ctrl+C")
    print()

    # Open browser in background thread
    threading.Thread(target=open_browser, args=(port,), daemon=True).start()

    # Launch the app inside the venv
    try:
        subprocess.check_call([py, os.path.join(ROOT, "app.py"), "--port", str(port)])
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()
