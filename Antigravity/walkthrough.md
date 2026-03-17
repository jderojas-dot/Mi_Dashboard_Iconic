# Walkthrough: Backend Startup Fixed

I have resolved the issues preventing the backend from starting.

## Changes Made

1. **Credentials Configuration**: Updated [backend/config.py](file:///c:/Dashboard_Iconic/Antigravity/backend/config.py) to automatically detect and use the [credentials.json](file:///c:/Dashboard_Iconic/credentials.json) file found in the project root.
2. **Startup Instructions**: Identified the correct command to run the server using the existing virtual environment.

## Verification Results

The server now starts correctly and connects to BigQuery:

```text
INFO:     Will watch for changes in these directories: ['C:\\Dashboard_Iconic\\Antigravity\\backend']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [11668] using WatchFiles
INFO:     Started server process [1844]
INFO:     Waiting for application startup.
✅ BigQuery conectado → proyecto: dashboard-iconic-terroirs
INFO:     Application startup complete.
```

## How to Run it Now

To start the server yourself, copy and paste this single line in your terminal (starting from the `C:\Dashboard_Iconic` folder):

```powershell
cd Antigravity/backend; ..\venv\Scripts\python.exe -m uvicorn main:app --reload --port 8000
```
