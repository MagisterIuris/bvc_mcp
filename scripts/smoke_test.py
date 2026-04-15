"""
Smoke test for the BVC MCP server HTTP/SSE mode.

Usage:
    # Test local server (start it first with: python -m bvc_mcp.server)
    python scripts/smoke_test.py

    # Test a remote deployment
    python scripts/smoke_test.py https://your-app.railway.app
"""
import asyncio
import sys

import httpx


async def smoke_test(base_url: str = "http://localhost:8000") -> None:
    print(f"Smoke testing {base_url} ...")
    async with httpx.AsyncClient(timeout=10) as client:
        # Test /health
        r = await client.get(f"{base_url}/health")
        assert r.status_code == 200, f"Health check failed: {r.status_code}"
        data = r.json()
        assert data.get("status") == "ok", f"Unexpected health payload: {data}"
        print(f"  /health → {data['status']}")

        # Test /sse endpoint is reachable (just check headers, don't consume stream)
        async with client.stream(
            "GET",
            f"{base_url}/sse",
            headers={"Accept": "text/event-stream"},
            timeout=5,
        ) as r:
            assert r.status_code == 200, f"SSE endpoint failed: {r.status_code}"
            print("  /sse   → reachable")

    print("All smoke tests passed.")


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    asyncio.run(smoke_test(url))
