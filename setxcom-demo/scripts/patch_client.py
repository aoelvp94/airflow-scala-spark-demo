"""Apply jsonValue() to 1.0.0 execution/Client.kt setXCom and setTaskStateStore only."""
from __future__ import annotations

import re
import sys
from pathlib import Path


def _replace_in_fun(src: str, fun_name: str) -> str:
    needle = f"override fun {fun_name}"
    start = src.find(needle)
    if start < 0:
        raise SystemExit(f"could not find {needle}")
    brace = src.find("{", start)
    if brace < 0:
        raise SystemExit(f"could not find body of {fun_name}")
    depth = 0
    i = brace
    while i < len(src):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
        i += 1
    else:
        raise SystemExit(f"unbalanced braces in {fun_name}")
    body = src[brace : end + 1]
    new_body, n = re.subn(r"it\.value = value\b", "it.value = jsonValue(value)", body, count=1)
    if n != 1:
        raise SystemExit(f"expected one 'it.value = value' in {fun_name}, found {n}")
    return src[:brace] + new_body + src[end + 1 :]


def main() -> None:
    path = Path(sys.argv[1])
    src = path.read_text()
    src = _replace_in_fun(src, "setXCom")
    src = _replace_in_fun(src, "setTaskStateStore")
    path.write_text(src)


if __name__ == "__main__":
    main()
