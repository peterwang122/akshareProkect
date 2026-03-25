param(
    [string]$TaskName = "AkShareDailyCollection",
    [string]$RepoRoot = "C:\Users\Administrator\PycharmProjects\akshareProkect",
    [string]$PythonExe = "C:\Users\Administrator\miniconda3\envs\akshareProkect\python.exe",
    [string]$RunScriptPath = "C:\Users\Administrator\PycharmProjects\akshareProkect\scripts\run_daily_collection.ps1",
    [string]$AtTime = "17:00"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $RunScriptPath)) {
    throw "Run script not found: $RunScriptPath"
}

$triggerTime = [datetime]::ParseExact($AtTime, "HH:mm", $null)
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$RunScriptPath`" -RepoRoot `"$RepoRoot`" -PythonExe `"$PythonExe`""

$trigger = New-ScheduledTaskTrigger -Daily -At $triggerTime
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 8)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Write-Host "Scheduled task registered: $TaskName at $AtTime"
