# run_producer.ps1
# Keeps mock_direct.py running permanently.
# Restarts automatically after any crash, exit, or error.
# Registered as a Windows Task Scheduler task — runs on login, independent of any IDE.

$scriptDir   = "D:\Sanjay_Projects\Twitter_Sentiment_analysis"
$producerScript = "$scriptDir\producer\mock_direct.py"
$logFile     = "$scriptDir\producer_runner.log"
$maxLogBytes = 5MB

function Write-Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Add-Content -Path $logFile -Value $line
    Write-Host $line
}

# Rotate log if it grows beyond 5 MB
if ((Test-Path $logFile) -and (Get-Item $logFile).Length -gt $maxLogBytes) {
    Move-Item $logFile "$logFile.bak" -Force
}

Write-Log "=== Producer runner started (PID: $PID) ==="

while ($true) {
    Write-Log "Starting mock_direct.py ..."
    try {
        $proc = Start-Process -FilePath "python" `
                              -ArgumentList "`"$producerScript`"" `
                              -WorkingDirectory $scriptDir `
                              -PassThru `
                              -NoNewWindow
        Write-Log "Process started (PID: $($proc.Id))"
        $proc.WaitForExit()
        Write-Log "Producer exited (code: $($proc.ExitCode)). Restarting in 5s..."
    } catch {
        Write-Log "ERROR launching producer: $_. Retrying in 5s..."
    }
    Start-Sleep -Seconds 5
}
