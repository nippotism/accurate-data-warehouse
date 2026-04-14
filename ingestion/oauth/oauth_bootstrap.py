import os
import base64
from urllib.parse import urlencode
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import urllib.request
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
AUTH_URL = os.getenv("AUTH_URL")
TOKEN_URL = os.getenv("TOKEN_URL")
REDIRECT_URI = os.getenv("REDIRECT_URI")
SCOPE = os.getenv("SCOPE")


class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/" or parsed.path == "/login":
            params = urlencode({
                "response_type": "code",
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "scope": SCOPE,
            })
            url = f"{AUTH_URL}?{params}"
            self.respond(f'<h2>OAuth Login</h2><a href="{url}">Login via OAuth Provider</a>')

        elif parsed.path == "/callback":
            qs = parse_qs(parsed.query)
            error = qs.get("error", [None])[0]
            code = qs.get("code", [None])[0]

            if error:
                self.respond(f"OAuth Error: {error}", 400)
                return
            if not code:
                self.respond("No authorization code received", 400)
                return

            body = urlencode({
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
            }).encode()

            credentials = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

            req = urllib.request.Request(TOKEN_URL, data=body, method="POST", headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {credentials}",
            })

            try:
                with urllib.request.urlopen(req) as res:
                    token = json.loads(res.read())

                print("====================================")
                print("ACCESS TOKEN :", token.get("access_token"))
                print("REFRESH TOKEN:", token.get("refresh_token"))
                print("EXPIRES IN   :", token.get("expires_in"))
                print("====================================")

                self.respond("""
                    <h2>OAuth Success ✅</h2>
                    <p>Refresh token sudah diterima.</p>
                    <p><strong>SILAKAN TUTUP HALAMAN INI.</strong></p>
                    <p>Cek terminal untuk refresh_token.</p>
                """)
            except Exception as e:
                self.respond(f"Failed to exchange token: {e}", 500)

    def respond(self, html, status=200):
        body = html.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress default request logs


if __name__ == "__main__":
    print("OAuth bootstrap server running at http://localhost:3000")
    print("Open http://localhost:3000/login to start OAuth login")
    HTTPServer(("", 3000), OAuthHandler).serve_forever()