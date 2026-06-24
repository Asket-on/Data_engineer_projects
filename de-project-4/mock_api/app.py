import os
import json
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

PORT = 5001
STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage")

class MockAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path.strip('/')
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Helper to clean query param values (remove surrounding quotes if any)
        def get_param(name):
            val = query_params.get(name)
            if not val:
                return None
            cleaned = val[0].strip("'\"")
            return cleaned

        if path == "couriers":
            file_path = os.path.join(STORAGE_DIR, "couriers.json")
            if not os.path.exists(file_path):
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"couriers.json not found. Run generate_data.py first.")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Offset and limit
            offset = int(get_param("offset") or 0)
            limit = int(get_param("limit") or 50)

            # Paginate
            response_data = data[offset : offset + limit]

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode("utf-8"))

        elif path == "deliveries":
            file_path = os.path.join(STORAGE_DIR, "deliveries.json")
            if not os.path.exists(file_path):
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"deliveries.json not found. Run generate_data.py first.")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Filter by "from"
            from_param = get_param("from")
            if from_param:
                # Expecting format YYYY-MM-DD HH:MM:SS
                try:
                    from_dt = datetime.strptime(from_param, "%Y-%m-%d %H:%M:%S")
                    filtered_data = []
                    for item in data:
                        item_dt = datetime.strptime(item["delivery_ts"], "%Y-%m-%d %H:%M:%S")
                        if item_dt >= from_dt:
                            filtered_data.append(item)
                    data = filtered_data
                except Exception as e:
                    # Fallback to string comparison if datetime parsing fails
                    data = [item for item in data if item["delivery_ts"] >= from_param]

            # Offset and limit
            offset = int(get_param("offset") or 0)
            limit = int(get_param("limit") or 50)

            response_data = data[offset : offset + limit]

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Endpoint not found. Use /couriers or /deliveries")

def run(server_class=HTTPServer, handler_class=MockAPIHandler):
    server_address = ('', PORT)
    httpd = server_class(server_address, handler_class)
    print(f"Starting mock API server on port {PORT}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        print("Server stopped.")

if __name__ == "__main__":
    run()
