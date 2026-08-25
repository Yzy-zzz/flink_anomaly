@echo off
setlocal
if exist "src\main\java\cn\ac\iie\anomaly\source\DorisPollingAlertSource.java" (
  echo [WARN] Found legacy DorisPollingAlertSource.java in this directory.
  echo [WARN] pom.xml excludes it from the Kafka build, but using a fresh directory is recommended.
)
call mvn clean test package
if errorlevel 1 exit /b %errorlevel%
echo.
echo Build complete: target\NetTrafficSentinel-1.0-SNAPSHOT.jar
