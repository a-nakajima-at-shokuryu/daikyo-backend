from pathlib import Path 
from functools import reduce 
import sys 

def fullpath(*path):
  root = Path(__file__).parent 
  path = reduce(lambda a, b: Path(a) / Path(b), path, root)
  return path.resolve().absolute()

root = str(fullpath())
if root not in sys.path: 
  sys.path.append(root)