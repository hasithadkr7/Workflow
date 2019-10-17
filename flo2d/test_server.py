from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import time

HOST_ADDRESS = '127.0.0.1'
HOST_PORT = 8080


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.timeout = 18000
        print('Handle GET request...')
        if self.path.startswith('/create-raincell'):
            print('create-raincell')
            time.sleep(60)
            response = {'response': 'create-raincell'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-sim-raincell'):
            print('create-sim-raincell')
            response = {'response': 'create-sim-raincell'}
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))


if __name__ == '__main__':
    try:
        print('starting server...')
        server_address = (HOST_ADDRESS, HOST_PORT)
        httpd = HTTPServer(server_address, StoreHandler)
        print('running server...')
        httpd.serve_forever()
    except Exception as e:
        print(str(e))


