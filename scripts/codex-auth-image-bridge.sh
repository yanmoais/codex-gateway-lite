#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  codex-auth-image-bridge.sh --prompt "..." [options]
  codex-auth-image-bridge.sh --status [--codex-home <dir>]

Options:
  --prompt <text>       Prompt to send to Codex imagegen. If omitted, stdin is used.
  --image <path>        Optional reference/edit image. Can be repeated.
  --out-dir <dir>       Directory where generated files are copied.
  --codex-home <dir>    Isolated auth CODEX_HOME. Defaults to $CODEX_AUTH_IMAGE_HOME or ~/.codex-auth-image.
  --workdir <dir>       Working directory for codex exec. Defaults to current directory.
  --status              Show login status for the isolated CODEX_HOME.
  --dry-run             Print the planned command and prompt, without running Codex.
  -h, --help            Show this help.

This bridge intentionally does not copy auth.json, cookies, tokens, or Electron profiles.
Log in to the isolated CODEX_HOME explicitly before generation.
USAGE
}

prompt=""
out_dir=""
codex_home="${CODEX_AUTH_IMAGE_HOME:-$HOME/.codex-auth-image}"
workdir="$PWD"
status_only=0
dry_run=0
images=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prompt)
      [[ $# -ge 2 ]] || { echo "Missing value for --prompt" >&2; exit 2; }
      prompt="$2"
      shift 2
      ;;
    --image)
      [[ $# -ge 2 ]] || { echo "Missing value for --image" >&2; exit 2; }
      images+=("$2")
      shift 2
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || { echo "Missing value for --out-dir" >&2; exit 2; }
      out_dir="$2"
      shift 2
      ;;
    --codex-home)
      [[ $# -ge 2 ]] || { echo "Missing value for --codex-home" >&2; exit 2; }
      codex_home="$2"
      shift 2
      ;;
    --workdir)
      [[ $# -ge 2 ]] || { echo "Missing value for --workdir" >&2; exit 2; }
      workdir="$2"
      shift 2
      ;;
    --status)
      status_only=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v codex >/dev/null 2>&1; then
  echo "codex CLI was not found in PATH." >&2
  exit 127
fi

codex_home="$(python3 - "$codex_home" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
workdir="$(python3 - "$workdir" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"

mkdir -p "$codex_home"

if [[ "$status_only" -eq 1 ]]; then
  echo "CODEX_HOME=$codex_home"
  CODEX_HOME="$codex_home" codex login status
  exit $?
fi

if [[ "$dry_run" -ne 1 ]]; then
  if ! CODEX_HOME="$codex_home" codex login status >/dev/null 2>&1; then
    cat >&2 <<EOF
The isolated Codex auth image environment is not logged in.

CODEX_HOME=$codex_home

Log in first with:
  CODEX_HOME="$codex_home" codex login

Or open the dedicated Codex Auth Image launcher and sign in there.
EOF
    exit 1
  fi
fi

if [[ -z "$prompt" ]]; then
  prompt="$(cat)"
fi

if [[ -z "${prompt//[[:space:]]/}" ]]; then
  echo "Missing prompt. Pass --prompt or pipe prompt text on stdin." >&2
  exit 2
fi

if [[ -z "$out_dir" ]]; then
  out_dir="$workdir/output/imagegen"
fi

out_dir="$(python3 - "$out_dir" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
mkdir -p "$out_dir"

image_count="${#images[@]}"
if [[ "$image_count" -gt 0 ]]; then
  for image in "${images[@]}"; do
    if [[ ! -f "$image" ]]; then
      echo "Image file not found: $image" >&2
      exit 2
    fi
  done
fi

generated_root="$codex_home/generated_images"
start_marker="$(python3 - <<'PY'
import time
print(time.time())
PY
)"

image_notes=""
if [[ "$image_count" -gt 0 ]]; then
  image_notes=$'Attached image roles:\n'
  index=1
  for image in "${images[@]}"; do
    image_notes+="- image ${index}: reference or edit input provided with -i"$'\n'
    index=$((index + 1))
  done
fi

request="$(python3 - "$prompt" "$out_dir" <<'PY'
import sys
prompt = sys.argv[1].strip()
out_dir = sys.argv[2]
print(f"""$imagegen

Create the requested image asset using the built-in Codex image generation tool.

Prompt:
{prompt}

Output handling:
- It is acceptable for Codex to save the raw generated image under CODEX_HOME/generated_images.
- Do not read or modify any credentials, auth files, cookies, or gateway configuration.
- After generation, report the generated file path if available.
- The wrapper will copy new generated files into: {out_dir}
""")
PY
)"

codex_args=(exec --sandbox read-only -C "$workdir")
if [[ "$image_count" -gt 0 ]]; then
  for image in "${images[@]}"; do
    codex_args+=(-i "$image")
  done
fi
codex_args+=(-)

if [[ "$dry_run" -eq 1 ]]; then
  echo "CODEX_HOME=$codex_home"
  printf 'codex'
  printf ' %q' "${codex_args[@]}"
  printf '\n\n'
  printf '%s\n' "$request"
  exit 0
fi

printf '%s\n%s\n' "$image_notes" "$request" | CODEX_HOME="$codex_home" codex "${codex_args[@]}"

python3 - "$generated_root" "$out_dir" "$start_marker" <<'PY'
from pathlib import Path
import shutil
import sys

generated_root = Path(sys.argv[1])
out_dir = Path(sys.argv[2])
start_marker = float(sys.argv[3])

extensions = {".png", ".jpg", ".jpeg", ".webp"}
copied = []
if generated_root.exists():
    for path in generated_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in extensions:
            continue
        if path.stat().st_mtime + 0.001 < start_marker:
            continue
        target = out_dir / path.name
        if target.exists():
            stem = target.stem
            suffix = target.suffix
            n = 2
            while True:
                candidate = out_dir / f"{stem}-{n}{suffix}"
                if not candidate.exists():
                    target = candidate
                    break
                n += 1
        shutil.copy2(path, target)
        copied.append(target)

if copied:
    print("Copied generated image files:")
    for path in copied:
        print(path)
else:
    print(f"No new generated image files were found under {generated_root}.")
    print("If generation succeeded, inspect the Codex run output above for the saved path.")
PY
