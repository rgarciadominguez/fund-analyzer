import webbrowser
import os
import glob
import time

files = sorted(glob.glob("dashboard/fund-*.html"))
print(f"Abriendo {len(files)} dashboards:")
for f in files:
    abs_path = os.path.abspath(f).replace("\\", "/")
    url = "file:///" + abs_path
    print(f"  {os.path.basename(f)}")
    webbrowser.open(url, new=2)
    time.sleep(0.5)
print("Done")
