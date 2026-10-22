
$ErrorActionPreference = "Stop"

$env:JAVA_HOME = "C:\Java17"
$env:HADOOP_HOME = "C:\hadoop-3.5.0"
$env:SPARK_HOME = "C:\spark-4.2.0-bin-hadoop3"

$env:PYSPARK_PYTHON = "C:\Users\user\AppData\Local\Programs\Python\Python312\python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON

$env:PATH = "$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:HADOOP_HOME\sbin;$env:JAVA_HOME\bin;$env:PATH"

Set-Location $PSScriptRoot

& $env:PYSPARK_PYTHON -m jupyter notebook
