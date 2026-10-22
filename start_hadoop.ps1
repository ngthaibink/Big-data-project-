$ErrorActionPreference = "Stop"

$env:HADOOP_HOME = "C:\hadoop-3.5.0"
$env:JAVA_HOME = "C:\Java17"
$env:PATH = "$env:HADOOP_HOME\bin;$env:HADOOP_HOME\sbin;$env:JAVA_HOME\bin;$env:PATH"

Write-Host "`n=== KIEM TRA HADOOP ===" -ForegroundColor Cyan
hadoop version

Write-Host "`n=== KIEM TRA HDFS ===" -ForegroundColor Cyan

$report = & "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -report 2>&1
$report | Select-String "Live datanodes"

if ($LASTEXITCODE -eq 0 -and ($report -match "Live datanodes")) {
    Write-Host "HDFS dang hoat dong." -ForegroundColor Green
} else {
    Write-Host "HDFS chua san sang. Hay kiem tra dich vu Hadoop." -ForegroundColor Yellow
    exit 1
}
