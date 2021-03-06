import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import urlparse, parse_qs
from raincelldat.gen_raincell import create_hybrid_raincell
from inflowdat.get_inflow import create_inflow
from outflowdat.gen_outflow import create_outflow
from os.path import join as pjoin
from datetime import datetime


HOST_ADDRESS = 'localhost'
HOST_PORT = 8088


def set_daily_dir(run_date, run_time):
    start_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    run_time = start_datetime.strftime('%H-%M-%S')
    dir_path = pjoin(os.getcwd(), 'output', run_date, run_time)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            print(str(e))
    print('set_daily_dir|dir_path : ', dir_path)
    return dir_path


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/create-raincell'):
            print('create-raincell')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_hybrid_raincell(dir_path, run_date, run_time, forward, backward)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-inflow'):
            print('create-inflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_inflow(dir_path, run_date, run_time)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-outflow'):
            print('create-outflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_outflow(dir_path, run_date, run_time, forward, backward)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/run-flo2d'):
            print('run-flo2d')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_hybrid_raincell(dir_path, run_date, run_time, forward, backward)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/extract-data'):
            print('extract-data')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_outflow(run_date, run_time, forward, backward)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
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


