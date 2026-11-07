#!/usr/bin/env python3
"""Launch unmodified Scala sources with sbt's exported runtime classpath.

Run `sbt -batch compile 'export Runtime/fullClasspath' > /tmp/rat-classpath`
in spark/ first. Bypasses sbt's resident JVM, yielding a conservative baseline.
"""
import argparse
import os
from pathlib import Path
import re
import shutil

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--classpath-file", type=Path, required=True)
p.add_argument("--main", default="rat.Correlate")
args = p.parse_args()
root = Path(__file__).resolve().parents[2]
classpath = next(line for line in reversed(args.classpath_file.read_text().splitlines())
                 if line.startswith("/") and ".jar" in line)
source = (root / "spark/build.sbt").read_text().split("val sparkJavaOptions = Seq(", 1)[1].split("\n)", 1)[0]
options = re.findall(r'^\s*"([^"\n]+)"', source, re.MULTILINE)
java = shutil.which("java")
if not java:
    raise SystemExit("java must be on PATH (JDK 17)")
os.execv(java, [java, *options, "-cp", classpath, args.main])
