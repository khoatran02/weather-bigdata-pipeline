"""spark-submit needs a file; this runs a module of the installed package."""

import runpy
import sys

if __name__ == "__main__":
    module = sys.argv.pop(1)
    runpy.run_module(module, run_name="__main__", alter_sys=True)
