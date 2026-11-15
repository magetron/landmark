@echo off
rem Runs the Landmark server and opens it to the internet at https://landmark-server.patrickwu.uk
rem Needs the "landmark" Cloudflare tunnel (credentials in %USERPROFILE%\.cloudflared) and bin\cloudflared.exe.
rem Close both windows to stop.
cd /d "%~dp0"
start "Landmark server" python server.py
start "Landmark tunnel" bin\cloudflared.exe tunnel run --url http://localhost:8080 landmark
echo Friends open: https://landmark-server.patrickwu.uk
