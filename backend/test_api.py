import urllib.request
import urllib.error
import json

data = json.dumps({"email": "test@example.com"}).encode("utf-8")
req = urllib.request.Request("http://127.0.0.1:8000/api/v1/users", data=data, headers={"Content-Type": "application/json"})
try:
    res = urllib.request.urlopen(req)
    print("SUCCESS:", res.read().decode())
except urllib.error.HTTPError as e:
    print("ERROR HTTP:", e.code, e.read().decode())
except Exception as e:
    print("ERROR:", str(e))
