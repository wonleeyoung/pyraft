import socket
import time
import threading

class DataSenderClient:
    def __init__(self, server_ip, server_ports, interval=0.5):
        self.server_ip = server_ip
        self.server_ports = server_ports
        self.interval = interval
        self.socks = []
        self.data_num = 1
    def connect(self):
        for port in self.server_ports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.server_ip, port))
            self.socks.append(sock)
            print(f"Connected to {self.server_ip}:{port}")

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
            
    def send_one_data_if_needed(self, sock):
        self.data_num += 1
        data = str(self.data_num)
        sock.sendall(data.encode())
        print(f"Sent data to {sock.getpeername()}: {data}")
        self.data_num += 1
        time.sleep(self.interval)
    

    def start_sending(self):
        self.connect()
        threads = []
        for sock in self.socks:
            thread = threading.Thread(target=self.send_one_data_if_needed, args=(sock,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

if __name__ == "__main__":
    server_ip = "127.0.0.1"  # Replace with the actual IP of the RaftNode server
    server_ports = [5064, 5074, 5084, 5094, 5104, 5114, 5124]  # Replace with the actual ports where the RaftNode is listening for data
    interval = 0.5  # Interval in seconds

    client = DataSenderClient(server_ip, server_ports, interval)
    client.start_sending()
