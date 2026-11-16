#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
"$here/sbt.sh" -batch bundle submitJar
rm -rf "$here/astro/include/scala-bundle" "$here/astro/include/spark-submit"
mkdir -p "$here/astro/include/scala-bundle" "$here/astro/include/spark-submit"
cp "$here"/scala-job/target/bundle/*.jar "$here/astro/include/scala-bundle/"
cp "$here"/scala-job/target/submit/*.jar "$here/astro/include/spark-submit/"
ls "$here/astro/include/scala-bundle" | wc -l
ls "$here/astro/include/spark-submit"
