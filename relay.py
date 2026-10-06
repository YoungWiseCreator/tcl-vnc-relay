"""VNC relay bridge: pairs phone (reverse VNC) with viewer client.
Phone connects to PORT_PHONE, viewer connects to PORT_VIEWER.
Bridges bytes bidirectionally. Designed for Railway deployment.
"""
import socket, threading, os, time

PORT_PHONE = int(os.environ.get('PORT_PHONE', '5900'))
PORT_VIEWER = int(os.environ.get('PORT_VIEWER', '5901'))

phone_conn = None
viewer_conn = None
lock = threading.Lock()

def bridge(a, b, name):
    try:
        while True:
            data = a.recv(65536)
            if not data:
                break
            b.sendall(data)
    except Exception as e:
        print(f'bridge {name} ended: {e}', flush=True)
    finally:
        try: a.close()
        except: pass
        try: b.close()
        except: pass

def try_pair():
    global phone_conn, viewer_conn
    with lock:
        if phone_conn and viewer_conn:
            print('PAIRED! Bridging phone <-> viewer', flush=True)
            p, v = phone_conn, viewer_conn
            phone_conn, viewer_conn = None, None
            threading.Thread(target=bridge, args=(p, v, 'phone->viewer'), daemon=True).start()
            threading.Thread(target=bridge, args=(v, p, 'viewer->phone'), daemon=True).start()
            return True
    return False

def listen(port, label):
    global phone_conn, viewer_conn
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind('0.0.0.0', port)
    srv.listen(5)
    print(f'listening for {label} on 0.0.0.0:{port}', flush=True)
    while True:
        conn, addr = srv.accept()
        print(f'{label} connected from {addr}', flush=True)
        with lock:
            if label == 'phone':
                if phone_conn: phone_conn.close()
                phone_conn = conn
            else:
                if viewer_conn: viewer_conn.close()
                viewer_conn = conn
        try_pair()

if __name__ == '__main__':
    threading.Thread(target=listen, args=(PORT_PHONE, 'phone'), daemon=True).start()
    threading.Thread(target=listen, args=(PORT_VIEWER, 'viewer'), daemon=True).start()
    print('relay running', flush=True)
    while True:
        time.sleep(60)
