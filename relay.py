"""VNC relay: phone (raw TCP) <-> viewer (WebSocket).
Phone connects via raw TCP to PORT_PHONE (Railway TCP proxy).
Viewer connects via WebSocket to /vnc on the HTTP port (443 via proxy).
Bridges bytes bidirectionally.
"""
import socket, threading, os, time, asyncio, base64, hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT_PHONE = int(os.environ.get('PORT_PHONE', '5900'))
HTTP_PORT = int(os.environ.get('PORT', '8080'))

phone_conn = None
viewer_ws = None
lock = threading.Lock()

def bridge_tcp_to_ws(tcp_sock, ws_sock):
    try:
        while True:
            data = tcp_sock.recv(65536)
            if not data:
                break
            # WebSocket binary frame
            frame = bytes([0x82])
            ln = len(data)
            if ln < 126:
                frame += bytes([ln])
            elif ln < 65536:
                frame += bytes([126]) + ln.to_bytes(2, 'big')
            else:
                frame += bytes([127]) + ln.to_bytes(8, 'big')
            frame += data
            ws_sock.sendall(frame)
    except:
        pass
    finally:
        cleanup()

def bridge_ws_to_tcp(ws_sock, tcp_sock):
    try:
        while True:
            # Read WebSocket frame header
            hdr = ws_sock.recv(2)
            if len(hdr) < 2:
                break
            opcode = hdr[0] & 0x0F
            masked = (hdr[1] & 0x80) != 0
            length = hdr[1] & 0x7F
            if length == 126:
                length = int.from_bytes(ws_sock.recv(2), 'big')
            elif length == 127:
                length = int.from_bytes(ws_sock.recv(8), 'big')
            mask = ws_sock.recv(4) if masked else None
            payload = b''
            while len(payload) < length:
                chunk = ws_sock.recv(length - len(payload))
                if not chunk:
                    break
                payload += chunk
            if masked and mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:  # close
                break
            if opcode in (0x1, 0x2) and payload:  # text/binary
                tcp_sock.sendall(payload)
    except:
        pass
    finally:
        cleanup()

def cleanup():
    global phone_conn, viewer_ws
    with lock:
        for c in (phone_conn, viewer_ws):
            try:
                if c: c.close()
            except:
                pass
        phone_conn, viewer_ws = None, None

def try_pair():
    global phone_conn, viewer_ws
    with lock:
        if phone_conn and viewer_ws:
            print('PAIRED! Bridging phone <-> viewer', flush=True)
            p, v = phone_conn, viewer_ws
            phone_conn, viewer_ws = None, None
            threading.Thread(target=bridge_tcp_to_ws, args=(p, v), daemon=True).start()
            threading.Thread(target=bridge_ws_to_tcp, args=(v, p), daemon=True).start()

def phone_listener():
    global phone_conn
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind('0.0.0.0', PORT_PHONE)
    srv.listen(5)
    print(f'phone listener on 0.0.0.0:{PORT_PHONE}', flush=True)
    while True:
        conn, addr = srv.accept()
        print(f'phone connected from {addr}', flush=True)
        with lock:
            if phone_conn:
                try: phone_conn.close()
                except: pass
            phone_conn = conn
        try_pair()

class WSHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/vnc':
            # WebSocket handshake
            key = self.headers.get('Sec-WebSocket-Key', '')
            accept = base64.b64encode(
                hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()
            ).decode()
            self.send_response(101)
            self.send_header('Upgrade', 'websocket')
            self.send_header('Connection', 'Upgrade')
            self.send_header('Sec-WebSocket-Accept', accept)
            self.end_headers()
            print('viewer connected via WebSocket', flush=True)
            global viewer_ws
            with lock:
                if viewer_ws:
                    try: viewer_ws.close()
                    except: pass
                viewer_ws = self.connection
            try_pair()
            # Keep handler alive - bridging threads handle the data
            # Block until connection closes
            try:
                while True:
                    time.sleep(1)
                    # Check if still paired
                    with lock:
                        if viewer_ws is None:
                            break
            except:
                pass
        elif self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'ok')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass

if __name__ == '__main__':
    threading.Thread(target=phone_listener, daemon=True).start()
    print(f'HTTP/WebSocket on 0.0.0.0:{HTTP_PORT}', flush=True)
    HTTPServer(('0.0.0.0', HTTP_PORT), WSHandler).serve_forever()
