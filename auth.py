import os
from fastapi import Header, HTTPException

API_KEY = os.getenv("BACKEND_API_KEY")

def require_api_key(x_api_key: str = Header(default=None)):
    """
    Drop this as a dependency on any endpoint that reads/writes real money data
    or controls the autotrader, e.g.:

        @app.post("/api/autotrader/start", dependencies=[Depends(require_api_key)])

    The frontend must send the same key back as a header on every protected
    call: headers: { "X-API-Key": "<your key>" }
    """
    if not API_KEY:
        # Fails loud on startup misconfiguration rather than silently allowing everyone through
        raise HTTPException(status_code=500, detail="BACKEND_API_KEY not configured on server")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
