import sys, os, shutil
from pathlib import Path

print(sys.path)

cp_succeed = False
for p in sys.path:
    if "site-packages" in p:
        pysparkPath = Path(p + "/pyspark")
        pyspark_exist = True if pysparkPath.is_dir() else False
        if pyspark_exist:
            print("pyspark package already exist in {}".format(p))
            break
        else:
            shutil.copytree('/home/coder/project/extra_pkg/pyspark', p+"/pyspark")
            cp_succeed = True
            break

if cp_succeed:
    print(f"cp pyspark package to {p} succeed.")
else:
    print(f"cp pyspark to sys.path failed.")
            

