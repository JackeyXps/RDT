import math
import struct
import socket
from typing import Union
from USocket import UnreliableSocket
import threading
import time


class RDTSocket(UnreliableSocket):
    """
    The functions with which you are to build your RDT.
    -   recvfrom(bufsize)->bytes, addr
    -   sendto(bytes, address)
    -   bind(address)

    You can set the mode of the socket.
    -   settimeout(timeout)
    -   setblocking(flag)
    By default, a socket is created in the blocking mode. 
    https://docs.python.org/3/library/socket.html#socket-timeouts

    """

    def __init__(self, rate=None, debug=True, addr=None):
        super().__init__(rate=rate)
        self._rate = rate
        self._send_to = None
        self._recv_from = None
        self.debug = debug

        #############################################################################
        # TODO: ADD YOUR NECESSARY ATTRIBUTES HERE
        #############################################################################
        self.timeout = 1
        self.congWin = 1
        self.threshold = 100
        self.MSS = 1024
        self.maxWaitTime = 5
        self.seqNum = 0
        self.ackNum = 0
        self.sendSeqNum = 0
        self.sendAckNum = 0
        self.windowSize = 1000
        self.maxTimeout = 4
        self.started = False
        self.resendTimes = 0
        self.packetDict = {}
        self.packetDict_receive = {}
        self.receivePacketNum = 0
        self.ackDict_receive = {}
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def accept(self) -> ('RDTSocket', (str, int)):
        """
        Accept a connection. The socket must be bound to an address and listening for 
        connections. The return value is a pair (conn, address) where conn is a new 
        socket object usable to send and receive data on the connection, and address 
        is the address bound to the socket on the other end of the connection.

        This function should be blocking. 
        """
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
        print(data)
        print(addr)
        packet_receive = RDTProtocol.parse(data)
        while not packet_receive.syn:
            data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
            print(data)
            print(addr)
            packet_receive = RDTProtocol.parse(data)
        conn = RDTSocket(self._rate)
        conn.set_recv_from(addr)
        conn.set_send_to(addr)
        conn.seqNum = 1
        conn.ackNum = 1
        packet = RDTProtocol(seqNum=conn.seqNum,
                             ackNum=conn.ackNum, checksum=0, payload=None, syn=True, fin=False, ack=True)
        conn.sendto(packet.encode(), conn._recv_from)
        data, addr = conn.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
        print(data)
        print(addr)
        packet_receive = RDTProtocol.parse(data)
        while not packet_receive.ack or not packet_receive.ackNum == conn.seqNum:
            print('packet_receive.ackNum: %d' % packet_receive.ackNum)
            print(data)
            print(addr)
            conn.sendto(packet.encode(), conn._recv_from)
            data, addr = conn.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
            packet_receive = RDTProtocol.parse(data)
        conn.ackNum = 2
        conn.started = True
        threading.Thread(target=conn.receivePacket).start()
        print('server: Connection established')
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        return conn, addr

    def connect(self, address: (str, int)):
        """
        Connect to a remote socket at address.
        Corresponds to the process of establishing a connection on the client side.
        """
        #############################################################################
        # TODO: YOUR CODE HERE
        self.sendSeqNum = self.sendAckNum = 0
        self._send_to = address
        print('Connect to %s:%s' % address)
        startTime = time.perf_counter()
        self.sendSeqNum = 1
        threading.Thread(target=self.count).start()
        self.started = True
        # seqNum: int, ackNum: int, checksum: int, payload: bytes, syn: bool = False, fin: bool = False, ack:bool = False
        packet = RDTProtocol(seqNum=self.sendSeqNum,
                             ackNum=self.sendAckNum, checksum=0, payload=b's', syn=True, fin=False, ack=False)
        self.sendto(packet.encode(), self._send_to)
        data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
        packet_receive = RDTProtocol.parse(data)
        while not packet_receive.ack or not packet_receive.syn or not packet_receive.ackNum == self.sendSeqNum:
            print('Client syn packet_receive.ackNum: %d' % packet_receive.ackNum)
            self.sendto(packet.encode(), self._send_to)
            data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
            packet_receive = RDTProtocol.parse(data)
        self._send_to = addr
        self._recv_from = self._send_to
        self.sendAckNum = 1
        self.sendSeqNum = 2
        packet = RDTProtocol(seqNum=self.sendSeqNum,
                             ackNum=self.sendAckNum, checksum=0, payload=b's', syn=False, fin=False, ack=True)
        self.sendto(packet.encode(), self._send_to)
        threading.Thread(target=self.receivePacket).start()
        print('client: Connection to %s:%s established' % self._send_to)
        #############################################################################
        # raise NotImplementedError()
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def recv(self, buffer_size: int) -> bytes:
        """
        Receive data from the socket. 
        The return value is a bytes object representing the data received. 
        The maximum amount of data to be received at once is specified by bufsize. 
        
        Note that ONLY data send by the peer should be accepted.
        In other words, if someone else sends data to you from another address,
        it MUST NOT affect the data returned by this function.
        """
        assert self._recv_from, "Connection not established yet. Use recvfrom instead."
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################
        data = b''
        while self.started and len(data) < buffer_size:
            while self.receivePacketNum == len(self.packetDict_receive):
                continue
            self.receivePacketNum = len(self.packetDict_receive)
            while self.ackNum in self.packetDict_receive:
                packet = self.packetDict_receive[self.ackNum]
                print('receive data packet seq:%d ack:%d payload:%d' % (packet.seqNum, packet.ackNum, len(packet.payload)))
                if len(data) + len(packet.payload) > buffer_size: # 可能有超过buffer_size的bug
                    return data
                data += packet.payload
                self.ackNum = (self.ackNum + len(packet.payload)) % RDTProtocol.SEQ_NUM_BOUND
                print('ackNum:%d' % self.ackNum)
                if packet.fin:
                    self.started = False
                    data = b''
                    break
            packet = RDTProtocol(seqNum=self.seqNum,
                                 ackNum=self.ackNum, checksum=0, payload=None, syn=False, fin=False, ack=True)
            self.sendto(packet.encode(), self._recv_from)
            # for k in dataList.keys():
            # RuntimeError: dictionary changed size during iteration
        # for k in list(self.packetDict_receive.keys()):
        #     if k > self.ackNum:
        #         self.packetDict_receive.pop(k)
        # return self.packetDict_receive.values()
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        return data

    def send(self, data: bytes):
        """
        Send data to the socket. 
        The socket must be connected to a remote socket, i.e. self._send_to must not be none.
        """
        assert self._send_to, "Connection not established yet. Use sendto instead."
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        packetNum = 0
        while packetNum * self.MSS < len(data):
            self.sendSeqNum = (self.sendSeqNum + len(data[packetNum * self.MSS: packetNum * self.MSS + self.MSS]))%RDTProtocol.SEQ_NUM_BOUND
            packet = RDTProtocol(seqNum=self.sendSeqNum, ackNum=self.sendAckNum, checksum=0,
                                 payload=data[packetNum * self.MSS: packetNum * self.MSS + self.MSS], syn=False,
                                 fin=False, ack=False)
            print('pkt:%d with %d bytes' % (packetNum, len(data[packetNum * self.MSS: packetNum * self.MSS + self.MSS])))
            self.packetDict[packet.seqNum] = packet
            packetNum += 1
        threading.Thread(target=self.sendPackets).start()
        #############################################################################
        # raise NotImplementedError()
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################

    def close(self):
        """
        Finish the connection and release resources. For simplicity, assume that
        after a socket is closed, neither futher sends nor receives are allowed.
        """
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################

        print('Close connection with %s:%s' % self._send_to)
        startTime = time.perf_counter()
        self.sendSeqNum += 1
        packet = RDTProtocol(seqNum=self.sendSeqNum,
                             ackNum=self.sendAckNum, checksum=0, payload=b'f', syn=False, fin=True, ack=False)
        self.sendto(packet.encode(), self._send_to)
        self.waitForAck(startTime, packet.seqNum)
        data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
        packet_receive = RDTProtocol.parse(data)
        packet = RDTProtocol(seqNum=self.sendSeqNum,
                             ackNum=self.sendAckNum, checksum=0, payload=None, syn=False, fin=False, ack=True)

        t_end = time.perf_counter() + self.maxWaitTime
        while not packet_receive.fin and time.perf_counter() < t_end:
            print('packet_receive.ackNum: %d' % packet_receive.ackNum)
            self.sendto(packet.encode(), self._recv_from)
            data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
            packet_receive = RDTProtocol.parse(data)

        self.started = False
        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        super().close()

    def set_send_to(self, send_to):
        self._send_to = send_to

    def set_recv_from(self, recv_from):
        self._recv_from = recv_from

    def sendPackets(self):
        startTime = time.perf_counter()
        count = 0
        seqNumList = sorted(self.packetDict)
        print(seqNumList)
        for seqNum in seqNumList:
            count += 1
            self.sendto(self.packetDict[seqNum].encode(), self._send_to)
            print('send seq:%d' % self.packetDict[seqNum].seqNum)
            if count % self.congWin == 0:
                self.waitForAck(startTime, self.packetDict[seqNum].seqNum)

    def waitForAck(self, startTime, sendSeqNum):
        # print('send '+str(self.sendSeqNum))
        ackFinish = False
        resendTimes = 0
        duplicateTimes = 0
        timeout = False
        while not ackFinish:
            try:
                self.settimeout(self.timeout)
                ackNum, syn = self.receiveAck()
                print('ack: %d' % ackNum)
                if syn:
                    print('Client disconnection')
                    packet = RDTProtocol(seqNum=2,
                                         ackNum=1, checksum=0, payload=b's', syn=True, fin=False,
                                         ack=True)
                    self.sendto(packet.encode(), self._send_to)
                if ackNum >= sendSeqNum:
                    self.sendAckNum = ackNum
                    ackFinish = True
                elif ackNum > self.sendAckNum:
                    self.sendAckNum = ackNum
                    duplicateTimes = 0
                    resendTimes = 0
                    timeout = False
                # fast retransmit
                elif ackNum == self.sendAckNum:
                    duplicateTimes += 1
                    if duplicateTimes == 3:
                        raise Exception

            except Exception as e:
                sendNum = 0
                for key in self.packetDict.keys():
                    if key > self.sendAckNum:
                        sendNum = key
                self.resendTimes += 1
                if isinstance(e, socket.timeout):
                    timeout = True
                print('seqNum: %d' % sendSeqNum)
                resendTimes += 1
                print('resend %d at %d times' % (sendNum, resendTimes))
                print('timeout ' + str(self.timeout) + 'sec')
                self.sendto(self.packetDict[sendNum].encode(), self._send_to)
                self.updataCongWin(True, timeout)
                self.updataTimeout(True)

        endTime = time.perf_counter()
        rtt = endTime - startTime
        self.updataCongWin(resendTimes != 0, timeout)
        self.updataTimeout(resendTimes != 0, rtt)

    def updataTimeout(self, resend, rtt=1):
        if resend == True:
            if self.timeout < self.maxTimeout:
                self.timeout *= 2
        else:
            self.timeout = 0.8 * self.timeout + 0.2 * rtt + 0.2 * rtt

    def updataCongWin(self, resend, timeout):
        if resend == True and self.congWin > 1:
            self.threshold = math.ceil(0.5 * self.congWin)
            if timeout == True:
                self.congWin = 1
            else:
                self.congWin = self.threshold
        elif self.congWin < self.windowSize:
            if self.congWin >= self.threshold:
                self.congWin += 1
            else:
                self.congWin *= 2

    def receiveAck(self):
        start = time.perf_counter()
        while len(self.ackDict_receive) == 0 and time.perf_counter() - start < self.timeout:
            continue
        if time.perf_counter() - start >= self.timeout:
            raise Exception(socket.timeout)
        else:
            seqNum, packet = self.ackDict_receive.popitem()
            return packet.ackNum, packet.syn

    def receivePacket(self):
        while True:
            if self.started:
                try:
                    data, addr = self.recvfrom(200 + RDTProtocol.SEGMENT_LEN)
                    packet = RDTProtocol.parse(data)
                    if addr == self._recv_from:
                        if packet.ack:
                            self.ackDict_receive[packet.seqNum] = packet
                            print('receive ack packet from %s %s' % addr)
                            print('seq:%d ack:%d' % (packet.seqNum, packet.ackNum))
                        elif packet.payload and not packet.syn:
                            self.packetDict_receive[packet.seqNum - len(packet.payload)] = packet
                            print('receive data packet from %s %s' % addr)
                            print('seq:%d ack:%d payloadLength:%d' % (packet.seqNum, packet.ackNum, len(packet.payload)))
                except Exception:
                    continue
            else:
                break
    def count(self):
        while True:
            last = self.sendSeqNum
            self.resendTimes = 0
            time.sleep(0.5)
            if self.started:
                pass
                # print('sending rate: %dKB/s' % ((self.sendSeqNum - last) * 2 / (1024)))
                # print('resend ratio: %.3f%%' %
                #       ((self.resendTimes * self.MSS * 100) / (self.sendSeqNum - last + 1)))
            else:
                break

"""

You can define additional functions and classes to do thing such as packing/unpacking packets, or threading.
"""


class RDTProtocol:
    """
    Reliable Data Transfer protocol Format:

      0   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |SYN|FIN|ACK|                      LEN                          |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                              SEQ #                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                              SEQ #                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                             SEQACK #                          |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                             SEQACK #                          |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                           CHECKSUM                            |
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |                                                               |
    /                            PAYLOAD                            /
    /                                                               /
    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+

    Flags:
     - SYN                      Synchronize
     - FIN                      Finish
     - ACK                      Acknowledge

    Ranges:
     - Payload Length           0 - 1024  (append zeros to the end if length < 512)
     - Sequence Number          0 - 2^32 - 1
     - Acknowledgement Number   0 - 2^32 - 1

    Checksum Algorithm:         16 bit one's complement of the one's complement sum

    Size of sender's window     1000
    """
    HEADER_LEN = 12
    MAX_PAYLOAD_LEN = 1024
    SEGMENT_LEN = MAX_PAYLOAD_LEN + HEADER_LEN
    SEQ_NUM_BOUND = pow(2, 32) - 1

    def __init__(self, seqNum: int, ackNum: int, checksum: int, payload: bytes, syn: bool = False, fin: bool = False,
                 ack: bool = False):
        self.syn = syn
        self.fin = fin
        self.ack = ack
        self.seqNum = seqNum % self.SEQ_NUM_BOUND
        self.ackNum = ackNum % self.SEQ_NUM_BOUND
        self.checksum = checksum
        if payload is not None and len(payload) > RDTProtocol.MAX_PAYLOAD_LEN:
            raise ValueError
        self.payload = payload

    def encode(self) -> bytes:
        """Returns fixed length bytes"""
        head = 0x0000 | len(self.payload) if self.payload else 0
        if self.syn:
            head |= 0x8000
        if self.fin:
            head |= 0x4000
        if self.ack:
            head |= 0x2000
        arr = bytearray(struct.pack('!HIIH', head, self.seqNum, self.ackNum, 0))
        if self.payload:
            arr.extend(self.payload)
        checksum = calc_checksum(arr)
        arr[10] = (checksum >> 8) & 0xFF
        arr[11] = checksum & 0xFF
        # arr.extend(b'\x00' * (RDTProtocol.SEGMENT_LEN - len(arr)))  # so that the total length is fixed
        return bytes(arr)

    @staticmethod
    def parse(segment: Union[bytes, bytearray]) -> 'RDTProtocol':
        """Parse raw bytes into an RDTSegment object"""
        try:
            # assert len(segment) == RDTProtocol.SEGMENT_LEN
            # assert 0 <= len(segment) - 12 <= RDTProtocol.MAX_PAYLOAD_LEN
            print('calc_checksum: %d' % calc_checksum(segment))
            assert calc_checksum(segment) == 0
            head, seq_num, ack_num, checksum = struct.unpack('!HIIH', segment[0:12])
            syn = (head & 0x8000) != 0
            fin = (head & 0x4000) != 0
            ack = (head & 0x2000) != 0
            length = head & 0x1FFF
            # assert length + 6 == len(segment)
            payload = segment[12:12 + length]
            return RDTProtocol(seq_num, ack_num, checksum, payload, syn, fin, ack)
        except AssertionError as e:
            raise ValueError from e


def calc_checksum(segment: Union[bytes, bytearray]) -> int:
    i = iter(segment)
    bytes_sum = sum(((a << 8) + b for a, b in zip(i, i)))  # for a, b: (s[0], s[1]), (s[2], s[3]), ...
    if len(segment) % 2 == 1:  # pad zeros to form a 16-bit word for checksum
        bytes_sum += segment[-1] << 8
    # add the overflow at the end (adding two times is sufficient)
    bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
    bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
    return ~bytes_sum & 0xFFFF
