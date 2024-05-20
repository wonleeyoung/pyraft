import socket

def udp_listener(ip, port):
    # UDP 소켓 생성
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # IP와 포트를 바인딩
    udp_sock.bind((ip, port))
    
    print(f"Listening for UDP packets on {ip}:{port}")
    
    try:
        while True:
            # 패킷 수신
            data, addr = udp_sock.recvfrom(1024)  # 버퍼 크기 1024 바이트
            print(f"Received message from {addr}: {data.decode()}")
    
    except KeyboardInterrupt:
        print("UDP listener stopped.")
    
    finally:
        udp_sock.close()

# 사용 예시
if __name__ == "__main__":
    IP = "172.19.50.19"  # 모든 인터페이스에서 수신
    PORT = 5400    # 수신할 포트
    udp_listener(IP, PORT)
