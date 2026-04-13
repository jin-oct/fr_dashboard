$ErrorActionPreference = "Stop"

Set-Location "D:\CODEX\DASHBOARD"

$git = "C:\Program Files\Git\cmd\git.exe"
$python = "python"
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

Write-Output "[$timestamp] generate start"
& $python "generate.py"

Write-Output "[$timestamp] stage files"
& $git add `
  "docs/index.html" `
  "docs/data.json" `
  "docs/.nojekyll" `
  "index.html" `
  "data.json" `
  "funding_dataset.json" `
  "template.html" `
  "generate.py" `
  "GITHUB_PAGES_SETUP.md" `
  ".gitignore" `
  "publish.ps1"

$changes = & $git diff --cached --name-only
if (-not $changes) {
  Write-Output "no changes"
  exit 0
}

$commitTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
& $git commit -m "auto update $commitTime"
& $git push origin main

Write-Output "[$commitTime] publish done"
