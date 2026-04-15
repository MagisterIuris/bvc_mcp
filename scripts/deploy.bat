@echo off
echo === BVC MCP Server - Railway Deploy ===

echo 1. Running tests...
python -m pytest tests/ -q --tb=short
if errorlevel 1 (
    echo Tests FAILED. Aborting deploy.
    exit /b 1
)
echo Tests passed.

echo 2. Pushing to Railway...
railway up
if errorlevel 1 (
    echo Railway deploy FAILED.
    exit /b 1
)
echo Deploy complete.

echo 3. Checking logs...
timeout /t 5 /nobreak >nul
railway logs --tail 20
