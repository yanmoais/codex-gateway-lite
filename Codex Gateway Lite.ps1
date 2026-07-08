$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls11 -bor [Net.SecurityProtocolType]::Tls } catch {}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path $ScriptDir
$ConfigDir = Join-Path $env:USERPROFILE ".codex-gateway-lite"
$ConfigFile = Join-Path $ConfigDir "config.json"
$DebugPort = if ($env:CODEX_GATEWAY_LITE_DEBUG_PORT) { $env:CODEX_GATEWAY_LITE_DEBUG_PORT } else { "9229" }
$AppPath = $env:CODEX_GATEWAY_LITE_APP
$script:AgentStarted = $false
$LiteBin = if ($env:CODEX_GATEWAY_LITE_BIN) { $env:CODEX_GATEWAY_LITE_BIN } else { Join-Path $RepoRoot "target\release\codex-gateway-lite.exe" }
$RustupOfficialBase = "https://static.rust-lang.org"
$RustupUstcBase = "https://mirrors.ustc.edu.cn/rust-static"
$CratesIndexUrl = "https://index.crates.io/config.json"
$CodexStoreProductId = "9PLM9XGG6VKS"
$CodexStoreUrl = "https://apps.microsoft.com/detail/9PLM9XGG6VKS"
$CodexInstallerUrl = "https://get.microsoft.com/installer/download/9PLM9XGG6VKS?cid=website_cta_psi"

function Write-Header {
  Clear-Host
  Write-Host "╭────────────────────────────────────────────╮" -ForegroundColor Cyan
  Write-Host "│        Codex Gateway Lite Bootstrap        │" -ForegroundColor Cyan
  Write-Host "╰────────────────────────────────────────────╯" -ForegroundColor Cyan
  Write-Host "Project dir: $RepoRoot" -ForegroundColor DarkGray
  Write-Host "Config file: $ConfigFile" -ForegroundColor DarkGray
  Write-Host ""
}

function Write-Section([string]$Text) { Write-Host "`n▶ $Text" -ForegroundColor Cyan }
function Write-Ok([string]$Text) { Write-Host "  ✓ $Text" -ForegroundColor Green }
function Write-Info([string]$Text) { Write-Host "  • $Text" -ForegroundColor DarkGray }
function Write-Warn([string]$Text) { Write-Host "  ! $Text" -ForegroundColor Yellow }
function Fail([string]$Text) { Write-Host "`n  ✗ $Text" -ForegroundColor Red; exit 1 }

function Test-Command([string]$Name) {
  return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-UserCargoBin {
  return Join-Path $env:USERPROFILE ".cargo\bin"
}

function Add-CargoBinToPath {
  $cargoBin = Get-UserCargoBin
  if (-not (Test-Path $cargoBin)) { return }
  $pathParts = @($env:Path -split ';' | Where-Object { $_ -and $_.Trim().Length -gt 0 })
  if ($pathParts -notcontains $cargoBin) {
    $env:Path = "$cargoBin;$env:Path"
  }
}

function Ensure-CargoBinUserPath {
  $cargoBin = Get-UserCargoBin
  if (-not (Test-Path $cargoBin)) { return }
  $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
  $userParts = @($userPath -split ';' | Where-Object { $_ -and $_.Trim().Length -gt 0 })
  if ($userParts -notcontains $cargoBin) {
    [Environment]::SetEnvironmentVariable("Path", "$cargoBin;$userPath", "User")
  }
}

function Merge-BypassList([string]$Existing, [string[]]$Required) {
  $items = @()
  if ($Existing) {
    $items += @($Existing -split '[,;]' | Where-Object { $_ -and $_.Trim().Length -gt 0 } | ForEach-Object { $_.Trim() })
  }
  foreach ($item in $Required) {
    if (-not ($items | Where-Object { $_ -ieq $item } | Select-Object -First 1)) {
      $items += $item
    }
  }
  return ($items -join ',')
}

function Publish-UserEnvironmentChange {
  try {
    if (-not ("CodexGatewayLiteNativeMethods" -as [type])) {
      Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class CodexGatewayLiteNativeMethods {
  [DllImport("user32.dll", SetLastError = true, CharSet = CharSet.Auto)]
  public static extern IntPtr SendMessageTimeout(
    IntPtr hWnd,
    uint Msg,
    UIntPtr wParam,
    string lParam,
    uint fuFlags,
    uint uTimeout,
    out UIntPtr lpdwResult);
}
"@
    }
    $result = [UIntPtr]::Zero
    [CodexGatewayLiteNativeMethods]::SendMessageTimeout(
      [IntPtr]0xffff,
      0x001A,
      [UIntPtr]::Zero,
      "Environment",
      0x0002,
      5000,
      [ref]$result
    ) | Out-Null
  } catch {
    Write-Warn "用户环境变量已写入，但广播环境变更失败；如 Codex App 仍旧走代理，请重新登录 Windows 后再试。"
  }
}

function Ensure-LocalProxyBypass {
  $required = @('localhost', '127.0.0.1', '::1')
  $existingValues = @(
    $env:NO_PROXY,
    $env:no_proxy,
    [Environment]::GetEnvironmentVariable("NO_PROXY", "User"),
    [Environment]::GetEnvironmentVariable("no_proxy", "User")
  ) | Where-Object { $_ -and $_.Trim().Length -gt 0 }
  $existing = ($existingValues -join ',')
  $merged = Merge-BypassList $existing $required
  $env:NO_PROXY = $merged
  $env:no_proxy = $merged
  $changed = $false
  if ([Environment]::GetEnvironmentVariable("NO_PROXY", "User") -ne $merged) {
    [Environment]::SetEnvironmentVariable("NO_PROXY", $merged, "User")
    $changed = $true
  }
  if ([Environment]::GetEnvironmentVariable("no_proxy", "User") -ne $merged) {
    [Environment]::SetEnvironmentVariable("no_proxy", $merged, "User")
    $changed = $true
  }
  if ($changed) {
    Publish-UserEnvironmentChange
  }
  Write-Info "本地代理绕过已设置：localhost / 127.0.0.1 / ::1（当前进程 + 用户环境）"
}

function Test-Url([string]$Url) {
  try {
    Invoke-WebRequest -Uri $Url -UseBasicParsing -Method Head -TimeoutSec 8 | Out-Null
    return $true
  } catch {
    try {
      Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 8 | Out-Null
      return $true
    } catch {
      return $false
    }
  }
}

function Use-ChinaMirror {
  $mirrorMode = if ($env:CODEX_GATEWAY_LITE_USE_CN_MIRROR) { $env:CODEX_GATEWAY_LITE_USE_CN_MIRROR } else { "auto" }
  switch ($mirrorMode) {
    { $_ -in @("1", "true", "TRUE", "yes", "YES", "cn", "CN", "china", "CHINA") } { return $true }
    { $_ -in @("0", "false", "FALSE", "no", "NO", "off", "OFF") } { return $false }
  }
  return -not (Test-Url "$RustupOfficialBase/rustup/dist/channel-rust-stable.toml")
}

function Get-RustHostTriple {
  $arch = $env:PROCESSOR_ARCHITECTURE
  if ($arch -eq "ARM64") { return "aarch64-pc-windows-msvc" }
  return "x86_64-pc-windows-msvc"
}

function Download-File([string]$Url, [string]$Destination, [string]$Label) {
  Write-Info "下载 $Label"
  Write-Info $Url
  $dir = Split-Path -Parent $Destination
  if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  $partial = "$Destination.download"
  Remove-Item -Force -ErrorAction SilentlyContinue $Destination, $partial
  try {
    Invoke-WebRequest -Uri $Url -UseBasicParsing -OutFile $partial -TimeoutSec 180
    if (-not (Test-Path $partial)) { throw "download produced no file" }
    Move-Item -Force $partial $Destination
  } catch {
    Remove-Item -Force -ErrorAction SilentlyContinue $Destination, $partial
    throw
  }
}

function Test-WindowsExe([string]$Path, [int64]$MinBytes) {
  if (-not (Test-Path $Path)) { return $false }
  $item = Get-Item $Path -ErrorAction SilentlyContinue
  if (($null -eq $item) -or ($item.Length -lt $MinBytes)) { return $false }
  $fs = $null
  try {
    $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
    $b0 = $fs.ReadByte()
    $b1 = $fs.ReadByte()
    return (($b0 -eq 0x4D) -and ($b1 -eq 0x5A))
  } catch {
    return $false
  } finally {
    if ($null -ne $fs) { $fs.Close() }
  }
}

function Install-RustupFromBase([string]$Base, [string]$Triple) {
  $url = "$Base/rustup/dist/$Triple/rustup-init.exe"
  $tempRoot = Join-Path $env:TEMP "codex-gateway-lite"
  New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
  $installer = Join-Path $tempRoot "rustup-init-$Triple-$([Guid]::NewGuid().ToString('N')).exe"
  try {
    Download-File $url $installer "Rustup ($Triple)"
    if (-not (Test-WindowsExe $installer 1048576)) {
      $size = if (Test-Path $installer) { (Get-Item $installer).Length } else { 0 }
      throw "downloaded rustup-init.exe is not a valid Windows executable; size=$size"
    }
    $env:RUSTUP_DIST_SERVER = $Base
    $env:RUSTUP_UPDATE_ROOT = "$Base/rustup"
    $proc = Start-Process -FilePath $installer -ArgumentList @("-y", "--default-toolchain", "stable", "--profile", "minimal") -Wait -PassThru
    if ($proc.ExitCode -ne 0) { throw "rustup-init.exe exited with code $($proc.ExitCode)" }
  } finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $installer, "$installer.download"
  }
}

function Ensure-Git {
  if (Test-Command git) {
    Write-Ok "Git 已可用： $((git --version) 2>$null)"
    return
  }
  Write-Warn "未检测到 Git，开始使用 winget 安装 Git for Windows。"
  if (-not (Test-Command winget)) {
    Fail "缺少 Git，且当前系统没有 winget。请手动安装 Git：https://git-scm.com/download/win"
  }
  winget install -e --id Git.Git --accept-package-agreements --accept-source-agreements
  if (-not (Test-Command git)) {
    $env:Path = "$env:ProgramFiles\Git\cmd;$env:Path"
  }
  if (-not (Test-Command git)) { Fail "Git 安装完成后仍找不到 git.exe，请重开终端或检查 PATH。" }
  Write-Ok "Git 安装完成"
}

function Test-VSBuildTools {
  if (Test-Command cl.exe) { return $true }
  $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
  if (Test-Path $vswhere) {
    $path = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>$null
    if ($path) { return $true }
  }
  return $false
}

function Install-VSBuildToolsViaDirectInstaller([string]$WorkloadArgs) {
  $installer = Join-Path $env:TEMP "vs_BuildTools.exe"
  Download-File "https://aka.ms/vs/17/release/vs_BuildTools.exe" $installer "Visual Studio Build Tools"
  $proc = Start-Process -FilePath $installer -ArgumentList $WorkloadArgs -Wait -PassThru
  if ($proc.ExitCode -notin @(0, 3010)) { Fail "Visual Studio Build Tools 安装失败：exit $($proc.ExitCode)" }
}

function Ensure-VSBuildTools {
  if (Test-VSBuildTools) {
    Write-Ok "Microsoft C++ Build Tools 已可用"
    return
  }
  Write-Warn "未检测到 Microsoft C++ Build Tools（缺少 C++ 桌面开发工作负载），开始安装 VC++ build tools。"
  $workloadArgs = "--wait --passive --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended --norestart"
  if (Test-Command winget) {
    winget install -e --id Microsoft.VisualStudio.2022.BuildTools --accept-package-agreements --accept-source-agreements --override $workloadArgs
  } else {
    Install-VSBuildToolsViaDirectInstaller $workloadArgs
  }
  if (-not (Test-VSBuildTools)) {
    # winget 只会在包本身有新版本时才把 --override 参数透传给安装器；如果 Build Tools
    # 这个包 ID 已经存在（哪怕没装 C++ 工作负载），winget 会当成"无可用升级"直接跳过，
    # 导致 C++ 工作负载始终没被补装。这时改用官方安装器以“修改现有安装”的方式直接补装。
    Write-Warn "winget 未能补齐 C++ 工作负载（已安装的 Build Tools 包判定为无需升级），改用官方安装器直接补装工作负载。"
    Install-VSBuildToolsViaDirectInstaller $workloadArgs
  }
  if (-not (Test-VSBuildTools)) {
    Fail "Microsoft C++ Build Tools 仍不可用。如果安装器要求重启，请重启后重新运行脚本。"
  }
  Write-Ok "Microsoft C++ Build Tools 安装完成"
}

function Ensure-Rust {
  Add-CargoBinToPath
  if ((Test-Command cargo) -and (Test-Command rustc)) {
    Write-Ok "Rust toolchain 已可用： $((cargo --version) 2>$null)"
    Ensure-CargoBinUserPath
    return
  }
  $triple = Get-RustHostTriple
  $useMirror = Use-ChinaMirror
  if ($useMirror) {
    Write-Warn "官方 Rust 源连通性较差或已强制启用国内镜像，优先使用 USTC Rustup 镜像。"
    $bases = @($RustupUstcBase, $RustupOfficialBase)
  } else {
    $bases = @($RustupOfficialBase, $RustupUstcBase)
  }

  $installed = $false
  $lastError = $null
  $seen = @{}
  foreach ($base in $bases) {
    if ($seen.ContainsKey($base)) { continue }
    $seen[$base] = $true
    try {
      Install-RustupFromBase $base $triple
      $installed = $true
      break
    } catch {
      $lastError = $_.Exception.Message
      Write-Warn "Rustup 安装源失败：$base"
      Write-Warn $lastError
    }
  }
  if (-not $installed) {
    Fail "Rust 自动安装失败。请删除 %TEMP%\codex-gateway-lite 后重试，或手动安装 Rust：https://rustup.rs/。最后错误：$lastError"
  }

  Add-CargoBinToPath
  Ensure-CargoBinUserPath
  if (-not (Test-Command cargo)) { Fail "Rust 安装完成后仍找不到 cargo.exe，请重开终端或检查 PATH。" }
  Write-Ok "Rust toolchain 安装完成： $((cargo --version) 2>$null)"
}

function Get-CargoFetchStampPath {
  return Join-Path $RepoRoot "target\.codex-gateway-lite\cargo-fetch.stamp"
}

function Test-CargoDepsFresh {
  $stamp = Get-CargoFetchStampPath
  if (-not (Test-Path $stamp)) { return $false }
  $stampTime = (Get-Item $stamp).LastWriteTimeUtc
  foreach ($path in @((Join-Path $RepoRoot "Cargo.toml"), (Join-Path $RepoRoot "Cargo.lock"))) {
    if ((Test-Path $path) -and ((Get-Item $path).LastWriteTimeUtc -gt $stampTime)) {
      return $false
    }
  }
  return $true
}

function Update-CargoFetchStamp {
  $stamp = Get-CargoFetchStampPath
  $stampDir = Split-Path -Parent $stamp
  New-Item -ItemType Directory -Force -Path $stampDir | Out-Null
  Set-Content -Path $stamp -Value (Get-Date).ToUniversalTime().ToString("o") -Encoding UTF8
}

function Get-CargoConfigPath {
  $cargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $env:USERPROFILE ".cargo" }
  return Join-Path $cargoHome "config.toml"
}

function Test-CargoSourceOverride {
  $cfg = Get-CargoConfigPath
  if (-not (Test-Path $cfg)) { return $false }
  $text = Get-Content $cfg -Raw
  return ($text -match '(?m)^\[source\.crates-io\]' -or $text -match 'replace-with\s*=')
}

function Configure-CargoMirror([switch]$Force) {
  $cfg = Get-CargoConfigPath
  $dir = Split-Path -Parent $cfg
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  if (Test-CargoSourceOverride) {
    Write-Ok "Cargo registry 配置已存在，保持用户现有设置"
    return
  }
  $mirrorEnv = if ($env:CODEX_GATEWAY_LITE_CARGO_MIRROR) { $env:CODEX_GATEWAY_LITE_CARGO_MIRROR } else { "auto" }
  if (-not $Force) {
    if ($mirrorEnv -in @("0", "false", "FALSE", "no", "NO", "off", "OFF")) {
      Write-Info "已按环境变量跳过 Cargo 国内镜像配置"
      return
    }
    if (($mirrorEnv -notin @("1", "true", "TRUE", "yes", "YES", "cn", "CN", "china", "CHINA")) -and (Test-Url $CratesIndexUrl)) {
      Write-Ok "crates.io sparse index 可访问"
      return
    }
  }
  Write-Warn "写入 Cargo rsproxy sparse 镜像： $cfg"
  Add-Content -Path $cfg -Value ""
  Add-Content -Path $cfg -Value "# Added by Codex Gateway Lite bootstrap"
  Add-Content -Path $cfg -Value "[source.crates-io]"
  Add-Content -Path $cfg -Value 'replace-with = "rsproxy-sparse"'
  Add-Content -Path $cfg -Value ""
  Add-Content -Path $cfg -Value "[source.rsproxy-sparse]"
  Add-Content -Path $cfg -Value 'registry = "sparse+https://rsproxy.cn/index/"'
}

function Ensure-CargoDeps {
  Configure-CargoMirror
  if (Test-CargoDepsFresh) {
    Write-Ok "Rust 依赖已就绪（跳过 cargo fetch）"
    return
  }
  Write-Info "预拉取 Rust 依赖（cargo fetch）"
  cargo fetch --manifest-path "Cargo.toml"
  if ($LASTEXITCODE -eq 0) {
    Update-CargoFetchStamp
    Write-Ok "Rust 依赖已就绪"
    return
  }
  Write-Warn "首次 cargo fetch 失败，启用 Cargo 国内镜像后重试。"
  Configure-CargoMirror -Force
  cargo fetch --manifest-path "Cargo.toml"
  if ($LASTEXITCODE -ne 0) { Fail "Rust 依赖拉取失败。请配置代理，或检查 Cargo 镜像设置。" }
  Update-CargoFetchStamp
  Write-Ok "Rust 依赖已就绪"
}

function Test-CodexApp {
  if ($AppPath -and (Test-Path $AppPath)) { return $true }
  try {
    $pkg = Get-AppxPackage -Name OpenAI.Codex* -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($pkg) { return $true }
  } catch {}
  $candidates = @(
    (Join-Path $env:LOCALAPPDATA "OpenAI\Codex\bin\Codex.exe"),
    (Join-Path $env:LOCALAPPDATA "OpenAI\Codex\Codex.exe"),
    (Join-Path $env:LOCALAPPDATA "Programs\OpenAI\Codex\Codex.exe")
  )
  foreach ($candidate in $candidates) {
    if (Test-Path $candidate) { return $true }
  }
  return $false
}

function Ensure-CodexApp {
  if (Test-CodexApp) {
    Write-Ok "Codex App 已可用"
    return
  }
  Write-Warn "未检测到 Codex App，开始从 Microsoft Store 官方源安装。"
  if (Test-Command winget) {
    winget install --source msstore --id $CodexStoreProductId --accept-package-agreements --accept-source-agreements
  }
  if (-not (Test-CodexApp)) {
    Write-Warn "自动商店安装未完成，打开官方安装页面。"
    Write-Info $CodexStoreUrl
    try {
      Start-Process "ms-windows-store://pdp/?productid=$CodexStoreProductId"
    } catch {
      Start-Process $CodexStoreUrl
    }
    Read-Host "请完成 Codex App 安装，然后按 Enter 继续"
  }
  if (-not (Test-CodexApp)) {
    Write-Warn "尝试下载 Microsoft 官方安装器。"
    $installer = Join-Path $env:TEMP "Codex.appinstaller"
    try {
      Download-File $CodexInstallerUrl $installer "Codex App installer"
      Start-Process -FilePath $installer -Wait
    } catch {
      Fail "Codex App 安装失败。请手动安装：$CodexStoreUrl，然后重新运行脚本。"
    }
  }
  if (-not (Test-CodexApp)) { Fail "Codex App 仍不可用。请手动安装：$CodexStoreUrl，然后重新运行脚本。" }
  Write-Ok "Codex App 安装完成"
}

function Run-AgentDiagnostics {
  Write-Section "agent 失败诊断"
  Write-Info "Codex App 自动识别："
  try {
    Ensure-LiteBinary
    & $LiteBin where-app
  } catch {
    Write-Warn "where-app 执行失败：$($_.Exception.Message)"
  }
  Write-Info "57321 端口占用："
  try {
    $portLines = netstat -ano | findstr ":57321"
    if ($portLines) {
      $portLines | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
    } else {
      Write-Ok "57321 未发现监听占用"
    }
  } catch {
    Write-Warn "netstat 检查失败：$($_.Exception.Message)"
  }
}

function Test-LiteBinaryStale {
  if (-not (Test-Path $LiteBin)) { return $true }
  $bin = Get-Item $LiteBin
  foreach ($path in @((Join-Path $RepoRoot "Cargo.toml"), (Join-Path $RepoRoot "Cargo.lock"))) {
    if ((Test-Path $path) -and ((Get-Item $path).LastWriteTimeUtc -gt $bin.LastWriteTimeUtc)) {
      return $true
    }
  }
  $newerSource = Get-ChildItem -Path (Join-Path $RepoRoot "src") -Recurse -Filter "*.rs" -File -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTimeUtc -gt $bin.LastWriteTimeUtc } |
    Select-Object -First 1
  return [bool]$newerSource
}

function Get-StaleAgentProcessIds {
  try {
    $procs = Get-CimInstance Win32_Process -Filter "Name='codex-gateway-lite.exe'" -ErrorAction SilentlyContinue
  } catch {
    return @()
  }
  if (-not $procs) { return @() }
  $ids = @()
  foreach ($proc in $procs) {
    $cmd = [string]$proc.CommandLine
    if ($cmd -and ($cmd -match [regex]::Escape($LiteBin))) {
      $ids += $proc.ProcessId
    }
  }
  return $ids
}

function Stop-StaleAgentProcesses {
  $ids = Get-StaleAgentProcessIds
  if ($ids.Count -eq 0) { return }
  Write-Warn "检测到残留的 codex-gateway-lite agent 进程，先停止再构建：$($ids -join ', ')"
  foreach ($processId in $ids) {
    try { Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue } catch {}
  }
  $deadline = (Get-Date).AddSeconds(10)
  while ((Get-Date) -lt $deadline) {
    if ((Get-StaleAgentProcessIds).Count -eq 0) { break }
    Start-Sleep -Milliseconds 300
  }
}

function Ensure-LiteBinary {
  if (Test-LiteBinaryStale) {
    Stop-StaleAgentProcesses
    Write-Info "构建 release 二进制（后续源码未变化会直接复用）"
    cargo build --quiet --release --manifest-path "Cargo.toml"
    if ($LASTEXITCODE -ne 0) { Fail "release 二进制构建失败；如仍提示拒绝访问，请手动结束 codex-gateway-lite.exe 进程后重试" }
    Write-Ok "release 二进制已就绪：$LiteBin"
  }
}

function Test-InteractiveLiteCommand([string[]]$ArgsList) {
  if ($ArgsList.Count -eq 0) { return $false }
  return $ArgsList[0] -in @("agent", "init")
}

function Run-Lite([string[]]$ArgsList) {
  Ensure-LiteBinary
  if (Test-InteractiveLiteCommand $ArgsList) {
    if ($ArgsList[0] -eq "agent") {
      Write-Info "agent 将保持前台运行；关闭此窗口或按 Ctrl+C 会停止 agent。"
    }
    & $LiteBin @ArgsList
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
      Write-Warn "codex-gateway-lite 子命令退出码：$exitCode"
      Write-Info "可手动复现：`"$LiteBin`" $($ArgsList -join ' ')"
      if ($ArgsList[0] -eq "agent") {
        Run-AgentDiagnostics
      }
      Fail "codex-gateway-lite 命令失败： $($ArgsList -join ' ')"
    }
    return
  }

  $runId = [Guid]::NewGuid().ToString("N")
  $stdoutFile = Join-Path $env:TEMP "codex-gateway-lite-$runId.out.log"
  $stderrFile = Join-Path $env:TEMP "codex-gateway-lite-$runId.err.log"
  try {
    & $LiteBin @ArgsList > $stdoutFile 2> $stderrFile
    $exitCode = $LASTEXITCODE
    if (Test-Path $stdoutFile) {
      Get-Content -Path $stdoutFile -Encoding UTF8 | ForEach-Object { Write-Host $_ }
    }
    if (Test-Path $stderrFile) {
      Get-Content -Path $stderrFile -Encoding UTF8 | ForEach-Object {
        if ($_ -and $_.Trim().Length -gt 0) { Write-Host $_ -ForegroundColor Red }
      }
    }
  } finally {
    Remove-Item -Force -ErrorAction SilentlyContinue $stdoutFile, $stderrFile
  }
  if ($exitCode -ne 0) {
    Write-Warn "codex-gateway-lite 子命令退出码：$exitCode"
    Write-Info "可手动复现：`"$LiteBin`" $($ArgsList -join ' ')"
    if ($ArgsList.Count -gt 0 -and $ArgsList[0] -eq "agent") {
      Run-AgentDiagnostics
    }
    Fail "codex-gateway-lite 命令失败： $($ArgsList -join ' ')"
  }
}
function Stop-AgentOnExit {
  if (-not $script:AgentStarted) { return }
  Write-Host "`n脚本退出，停止 Codex Gateway Lite agent..." -ForegroundColor Yellow
  try {
    if (Test-Path $LiteBin) {
      & $LiteBin stop-agent | Out-Null
    } else {
      cargo run --quiet --manifest-path "Cargo.toml" -- stop-agent | Out-Null
    }
  } catch {
  }
}

try {
  Write-Header
  Set-Location $RepoRoot
  Ensure-LocalProxyBypass

  Write-Section "1/3 环境检测与依赖准备"
  Ensure-Git
  Ensure-VSBuildTools
  Ensure-Rust
  Ensure-CargoDeps
  Ensure-CodexApp

  Write-Section "2/3 初始化 Codex Gateway Lite 配置"
  New-Item -ItemType Directory -Force -Path $ConfigDir | Out-Null
  Run-Lite @("stop-agent")
  Run-Lite @("init", "--config", $ConfigFile)

  Write-Section "3/3 启动 agent 并拉起 Codex App"
  $script:AgentStarted = $true
  if ($AppPath) {
    Run-Lite @("agent", "--config", $ConfigFile, "--app", $AppPath, "--debug-port", $DebugPort)
  } else {
    Run-Lite @("agent", "--config", $ConfigFile, "--debug-port", $DebugPort)
  }

  Write-Host "`nCodex Gateway Lite agent 已退出。" -ForegroundColor Green
} finally {
  Stop-AgentOnExit
}
