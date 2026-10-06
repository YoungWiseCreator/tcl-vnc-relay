"""VNC relay with protocol multiplexing on a single PORT.
- Phone (DroidVNC-NG reverse VNC) connects via raw TCP; first bytes are "RFB ...".
- Viewer connects via WebSocket (wss://host/vnc); first bytes are "GET ...".
- GET /health returns 200 ok.
Both ride on Railway's $PORT, so the TCP proxy and the HTTPS domain target
the same port. No port juggling, no env vars needed.
"""
import socket
import threading
import os
import base64
import hashlib
import time

PORT = int(os.environ.get('PORT', '8080'))

phone_sock = None      # phone's raw TCP socket
phone_pending = b''    # RFB banner bytes already read from phone
viewer_sock = None     # viewer's WebSocket socket
lock = threading.Lock()


def send_ws_frame(sock, data):
    frame = bytes([0x82])
    ln = len(data)
    if ln < 126:
        frame += bytes([ln])
    elif ln < 65536:
        frame += bytes([126]) + ln.to_bytes(2, 'big')
    else:
        frame += bytes([127]) + ln.to_bytes(8, 'big')
    sock.sendall(frame + data)


def cleanup():
    global phone_sock, phone_pending, viewer_sock
    with lock:
        for c in (phone_sock, viewer_sock):
            try:
                if c:
                    c.close()
            except Exception:
                pass
        phone_sock, phone_pending, viewer_sock = None, b'', None


def try_pair():
    global phone_sock, viewer_sock
    with lock:
        paired = phone_sock is not None and viewer_sock is not None
    if paired:
        # Start both bridge directions; they coordinate via shared state
        threading.Thread(target=_bridge_both, daemon=True).start()


def _bridge_both():
    global phone_sock, phone_pending, viewer_sock
    with lock:
        p, pending, v = phone_sock, phone_pending, viewer_sock
        phone_sock, phone_pending, viewer_sock = None, b'', None
    print('PAIRED! Bridging phone <-> viewer', flush=True)
    t1 = threading.Thread(target=_fwd_tcp_ws, args=(p, pending, v), daemon=True)
    t2 = threading.Thread(target=_fwd_ws_tcp, args=(v, p), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    for c in (p, v):
        try:
            c.close()
        except Exception:
            pass
    print('pairing ended', flush=True)


def _fwd_tcp_ws(p, pending, v):
    try:
        if pending:
            send_ws_frame(v, pending)
        while True:
            data = p.recv(65536)
            if not data:
                break
            send_ws_frame(v, data)
    except Exception:
        pass


def _fwd_ws_tcp(v, p):
    try:
        while True:
            hdr = v.recv(2)
            if len(hdr) < 2:
                break
            opcode = hdr[0] & 0x0F
            masked = (hdr[1] & 0x80) != 0
            length = hdr[1] & 0x7F
            if length == 126:
                length = int.from_bytes(v.recv(2), 'big')
            elif length == 127:
                length = int.from_bytes(v.recv(8), 'big')
            mask = v.recv(4) if masked else None
            payload = b''
            while len(payload) < length:
                chunk = v.recv(length - len(payload))
                if not chunk:
                    break
                payload += chunk
            if masked and mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:
                break
            if opcode in (0x1, 0x2) and payload:
                p.sendall(payload)
    except Exception:
        pass


def handle_phone(conn, first_bytes):
    global phone_sock, phone_pending
    print('phone (VNC) connected', flush=True)
    with lock:
        if phone_sock:
            try:
                phone_sock.close()
            except Exception:
                pass
        phone_sock, phone_pending = conn, first_bytes
    try_pair()


def handle_http(conn, request_data):
    global viewer_sock
    try:
        header = request_data.decode('iso-8859-1', errors='replace')
        lines = header.split('\r\n')
        parts = lines[0].split(' ') if lines else []
        path = parts[1] if len(parts) > 1 else '/'
        if path == '/health':
            conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok')
            conn.close()
            return
        if path != '/vnc':
            conn.sendall(b'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            conn.close()
            return
        key = ''
        for l in lines[1:]:
            if l.lower().startswith('sec-websocket-key:'):
                key = l.split(':', 1)[1].strip()
                break
        if not key:
            conn.sendall(b'HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n')
            conn.close()
            return
        accept = base64.b64encode(
            hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()
        ).decode()
        conn.sendall(('HTTP/1.1 101 Switching Protocols\r\n'
                      'Upgrade: websocket\r\n'
                      'Connection: Upgrade\r\n'
                      f'Sec-WebSocket-Accept: {accept}\r\n\r\n').encode())
        print('viewer (WebSocket) connected', flush=True)
        with lock:
            if viewer_sock:
                try:
                    viewer_sock.close()
                except Exception:
                    pass
            viewer_sock = conn
        try_pair()
        while True:
            time.sleep(1)
            with lock:
                if viewer_sock is not conn:
                    break
    except Exception as e:
        print(f'http handler error: {e}', flush=True)
    finally:
        # If we were never paired, ensure socket closed
        with lock:
            if viewer_sock is conn:
                pass  # still waiting for phone; keep open


def client_thread(conn, addr):
    try:
        conn.settimeout(15)
        first = conn.recv(4096)
        if not first:
            conn.close()
            return
        conn.settimeout(None)
        if first.startswith(b'RFB'):
            handle_phone(conn, first)
        elif first.startswith(b'GET') or first.startswith(b'POST'):
            data = first
            while b'\r\n\r\n' not in data:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
            handle_http(conn, data)
        else:
            print(f'unknown protocol from {addr}: {first[:16]!r}', flush=True)
            conn.close()
    except Exception as e:
        print(f'client thread error: {e}', flush=True)
        try:
            conn.close()
        except Exception:
            pass


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', PORT))
    srv.listen(50)
    print(f'relay multiplexing on 0.0.0.0:{PORT} (raw VNC + WebSocket)', flush=True)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=client_thread, args=(conn, addr), daemon=True).start()


if __name__ == '__main__':
    main()
