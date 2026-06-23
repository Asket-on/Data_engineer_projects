import http.server
import socketserver
import json
import urllib.parse
import os

PORT = 5001

# Dictionary to track task polling status
# task_id -> number of times it has been polled
task_polls = {}

class MockAPIHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Prevent logging pollution in terminal, but print core events
        print(f"MockAPI: {format % args}")

    def do_POST(self):
        parsed_url = urllib.parse.urlparse(self.path)
        if parsed_url.path == "/generate_report":
            # Read headers
            nickname = self.headers.get('X-Nickname', 'unknown')
            api_key = self.headers.get('X-API-KEY', 'unknown')
            print(f"Received generate_report request from nickname: {nickname}")
            
            # Respond with mock task_id
            response_data = {"task_id": "mock-task-12345"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
        else:
            self.send_error(404, "Not Found")

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Serve static files from storage directory
        if parsed_url.path.startswith("/storage/"):
            # Clean path and fetch file locally
            local_path = parsed_url.path.lstrip("/")
            if os.path.exists(local_path) and os.path.isfile(local_path):
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Length", str(os.path.getsize(local_path)))
                self.end_headers()
                with open(local_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, f"File {local_path} not found")
            return

        if parsed_url.path == "/get_report":
            task_id = query_params.get("task_id", [None])[0]
            if not task_id:
                self.send_error(400, "Missing task_id")
                return
            
            # Poll count tracker (simulate asynchronous generation delay)
            polls = task_polls.get(task_id, 0)
            if polls < 1:
                # First poll returns NOT_READY
                task_polls[task_id] = polls + 1
                response_data = {"status": "NOT_READY"}
                print(f"Polling task {task_id}: NOT_READY")
            else:
                # Second poll returns SUCCESS
                response_data = {
                    "status": "SUCCESS",
                    "data": {
                        "report_id": "mock-report-54321"
                    }
                }
                print(f"Polling task {task_id}: SUCCESS (report_id: mock-report-54321)")
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode('utf-8'))

        elif parsed_url.path == "/get_increment":
            report_id = query_params.get("report_id", [None])[0]
            date_param = query_params.get("date", [None])[0]
            if not report_id or not date_param:
                self.send_error(400, "Missing report_id or date")
                return
            
            # Parse date YYYY-MM-DDT00:00:00 -> YYYYMMDD
            # e.g. 2026-06-15T00:00:00 -> 20260615
            try:
                date_part = date_param.split("T")[0] # get YYYY-MM-DD
                formatted_date = date_part.replace("-", "") # YYYYMMDD
            except Exception:
                formatted_date = "default"

            inc_id = f"inc-{formatted_date}"
            print(f"Generating increment info for date {date_param} -> increment_id: {inc_id}")
            
            response_data = {
                "status": "SUCCESS",
                "data": {
                    "increment_id": inc_id
                }
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
        else:
            self.send_error(404, "Not Found")

# Run HTTP server
if __name__ == "__main__":
    # Ensure current directory matches script location for S3-mock relative pathing
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    
    # Pre-generate mock data if storage directory is empty or missing
    if not os.path.exists("storage"):
        import generate_data
        generate_data.generate_all_data()

    print(f"Starting Mock API server on port {PORT}...")
    handler = MockAPIHandler
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        httpd.serve_forever()
