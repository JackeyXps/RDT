from USocket import UnreliableSocket
from rdt import RDTSocket

if __name__ == '__main__':
    sock = RDTSocket(UnreliableSocket().bind(('127.0.0.1', 8080)))

    sock2 = RDTSocket(UnreliableSocket().bind(('127.0.0.1', 8081)))

    sock2.connect(('127.0.0.1', 8081))
    sock2.accept()

    sock2.sendto(b'hello', ('127.0.0.1', 8080))
    print(sock.recvfrom(2048))