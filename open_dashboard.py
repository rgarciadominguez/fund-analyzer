import webbrowser
import os
import sys

isin = sys.argv[1] if len(sys.argv) > 1 else "ES0175437039"
path = os.path.abspath(f"dashboard/fund-{isin}.html")
url = "file:///" + path.replace("\\", "/")
print(f"Opening: {url}")
webbrowser.open(url)
