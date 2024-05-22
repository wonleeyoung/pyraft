import socket
import time
import threading

class DataSenderClient:
    def __init__(self, server_ip, server_ports, interval=1.0, server_ipports=[]):
        self.server_ip = server_ip
        self.server_ports = server_ports
        self.interval = interval
        self.socks = []

    def connect(self):
        #for port in self.server_ports:
        #    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #    sock.connect((self.server_ip, port))
        #    self.socks.append(sock)
        #    print(f"Connected to {self.server_ip}:{port}")
        for ip,port in server_ipports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            self.socks.append(sock)
            print(f"Connected to {ip}:{port}")
        
        
    def send_data(self, sock):
        try:
            for i in range(1, 40000):  # Send data 1 to 5
                data = str(i)
                sock.sendall(data.encode())
                print(f"Sent data to {sock.getpeername()}: {data}")
                time.sleep(self.interval)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            sock.close()
            print(f"Connection closed for {sock.getpeername()}")

    def start_sending(self):
        self.connect()
        threads = []
        for sock in self.socks:
            thread = threading.Thread(target=self.send_data, args=(sock,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

if __name__ == "__main__":
    server_ip = "127.0.0.1"  # Replace with the actual IP of the RaftNode server
    server_ports = [5062, 5072, 5082, 5092, 5102]  # Replace with the actual ports where the RaftNode is listening for data
    interval = 1  # Interval in seconds
    server_ipports = [['192.168.1.105',5102],['192.168.1.101',5062],['192.168.1.102',5072],['192.168.1.103',5080],['192.168.1.104',5092]]

    client = DataSenderClient(server_ip, server_ports, interval, server_ipports)
    client.start_sending()
