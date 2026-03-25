param(
    [string]$RepoRoot = "C:\Users\Administrator\PycharmProjects\akshareProkect",
    [string]$PythonExe = "C:\Users\Administrator\miniconda3\envs\akshareProkect\python.exe",
    [string]$SchedulerUrl = "http://127.0.0.1:8765/health",
    [int]$SchedulerWaitSeconds = 60
)

$ErrorActionPreference = "Stop"

function Test-SchedulerHealth {
    param([string]$Url)
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300)
    }
    catch {
        return $false
    }
}

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found: $PythonExe"
}

if (-not (Test-Path $RepoRoot)) {
    throw "Repository root not found: $RepoRoot"
}

$runtimeDir = Join-Path $RepoRoot "runtime"
$logsDir = Join-Path $runtimeDir "logs"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
}

$stamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$taskLog = Join-Path $logsDir "scheduled_daily_runner_$stamp.log"

Push-Location $RepoRoot
try {
    if (-not (Test-SchedulerHealth -Url $SchedulerUrl)) {
        Start-Process -FilePath $PythonExe `
            -ArgumentList "ak_scheduler_service.py", "serve" `
            -WorkingDirectory $RepoRoot `
            -WindowStyle Hidden

        $startedAt = Get-Date
        do {
            Start-Sleep -Seconds 2
            if (Test-SchedulerHealth -Url $SchedulerUrl) {
                break
            }
        } while (((Get-Date) - $startedAt).TotalSeconds -lt $SchedulerWaitSeconds)

        if (-not (Test-SchedulerHealth -Url $SchedulerUrl)) {
            throw "AK scheduler service failed to become healthy within $SchedulerWaitSeconds seconds."
        }
    }

    & $PythonExe "run.py" "runner" "daily" *>> $taskLog
    if ($LASTEXITCODE -ne 0) {
        throw "daily runner exited with code $LASTEXITCODE. See log: $taskLog"
    }
}
finally {
    Pop-Location
}
