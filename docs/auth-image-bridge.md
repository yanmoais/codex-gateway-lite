# Auth Image Bridge

`codex-auth-image-bridge.sh` is a small local bridge for using Codex's built-in
image generation from an isolated logged-in Codex environment.

It is intended for this split:

- Gateway environment: daily Codex work, gateway model routing, local proxy, MCP
  injection, and normal development sessions.
- Auth image environment: a separate `CODEX_HOME` used only for Codex login
  state and built-in image generation.

The bridge never copies `auth.json`, cookies, tokens, Electron profiles, or
gateway configuration. Log in to the auth image environment explicitly.

## Data Boundaries

Keep these surfaces separate:

- `CODEX_HOME`
- Codex `auth.json`
- Codex `config.toml`
- session and state files
- `generated_images`
- Electron `--user-data-dir` when launching Codex App profiles

For macOS Codex App launches, `codex-gateway-lite --codex-home <dir>` maps the
home directory to a matching Electron profile:

- default `~/.codex` -> `~/Library/Application Support/Codex`
- `~/.codex-gateway` -> `~/Library/Application Support/Codex-Gateway`
- `~/.codex-auth-image` -> `~/Library/Application Support/Codex-Auth-Image`

Changing only `CODEX_HOME` is not enough for a full desktop profile split. Use a
separate Electron `--user-data-dir` as well when the Codex App UI is involved.

## First-Time Setup

Create and log in to the isolated auth image home:

```bash
export CODEX_AUTH_IMAGE_HOME="$HOME/.codex-auth-image"
mkdir -p "$CODEX_AUTH_IMAGE_HOME"
CODEX_HOME="$CODEX_AUTH_IMAGE_HOME" codex login
```

Check status without touching the gateway environment:

```bash
scripts/codex-auth-image-bridge.sh --status
```

Generation runs also preflight this login status and stop early when the
isolated auth image home is not logged in.

## Generate

Run from the project that should receive the generated assets:

```bash
scripts/codex-auth-image-bridge.sh \
  --prompt "A dark alchemy game UI item icon, ornate engraved metal frame, glowing rune glass, no text" \
  --out-dir output/imagegen
```

With a reference image:

```bash
scripts/codex-auth-image-bridge.sh \
  --image reference.png \
  --prompt "Create a matching icon in the same visual style, no text" \
  --out-dir output/imagegen
```

The raw Codex output may be written under:

```text
$CODEX_AUTH_IMAGE_HOME/generated_images
```

The bridge copies newly generated image files into `--out-dir` when it can find
them.

## Gateway Integration Shape

For a local service or OpenWebUI tool, keep the trust boundary narrow:

1. Gateway receives a request with prompt, optional reference images, size/style
   notes, and output directory.
2. Gateway calls this bridge as a subprocess with `CODEX_AUTH_IMAGE_HOME` set.
3. The bridge invokes `codex exec` under the auth image `CODEX_HOME`.
4. The auth image environment runs Codex built-in imagegen.
5. The bridge copies only generated image files back to the requested output
   directory.

Do not let the gateway read auth files directly. Do not merge the auth image
home into the gateway home.
