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

    def __init__(self, rate=None, debug=True, addr = None):
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
        # self.threshold
        self.MSS = 512
        self.seqNum = 0
        self.ackNum = 0
        self.sendAckNum = 0
        self.windowSize = 100
        self.maxTimeout = 4
        self.started = False
        self.resendTimes = 0
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
        conn = RDTSocket(self._rate)
        addr = self._recv_from
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
        self.started = True
        self.seqNum = self.ackNum = self.sendAckNum = 0
        self._send_to = address
        print('Send to %s:%s' % address)
        startTime = time.perf_counter()
        threading.Thread(target=self.count).start()
        # seqNum: int, ackNum: int, checksum: int, payload: bytes, syn: bool = False, fin: bool = False, ack:bool = False
        packet = RDTProtocol(seqNum=self.seqNum,
                             ackNum=self.ackNum, checksum=0, payload=None, syn=True, fin=False, ack=False)
        self.seqNum += 1
        self.sendto(packet.encode(), self._send_to)
        self.waitForAck({0: packet}, startTime)
        #############################################################################
        raise NotImplementedError()
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
        data = None
        assert self._recv_from, "Connection not established yet. Use recvfrom instead."
        #############################################################################
        # TODO: YOUR CODE HERE                                                      #
        #############################################################################

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
        startTime = time.perf_counter()
        threading.Thread(target=self.count).start()
        # seqNum: int, ackNum: int, checksum: int, payload: bytes, syn: bool = False, fin: bool = False, ack:bool = False
        packet = RDTProtocol(seqNum=self.seqNum,
                             ackNum=self.ackNum, checksum=0, payload=data, syn=True, fin=False, ack=False)
        self.seqNum += 1
        self.sendto(packet, self._send_to)
        self.waitForAck({0: packet}, startTime)
        #############################################################################
        raise NotImplementedError()
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

        #############################################################################
        #                             END OF YOUR CODE                              #
        #############################################################################
        super().close()

    def set_send_to(self, send_to):
        self._send_to = send_to

    def set_recv_from(self, recv_from):
        self._recv_from = recv_from

    def count(self):
        while True:
            last = self.seqNum
            self.resendTimes = 0
            time.sleep(0.5)
            if self.started and self.resendTimes <= 5:
                print('sending rate: %dKB/s' % ((self.seqNum-last)*2/(1024)))
                print('resend ratio: %.3f%%' %
                      ((self.resendTimes*self.MSS*100)/(self.seqNum-last+1)))
            else:
                break

    def waitForAck(self, packetDict, startTime):
        # print('send '+str(self.seqNum))
        ackFinish = False
        resendTimes = 0
        duplicateTimes = 0
        timeout = False
        while not ackFinish:
            try:
                self.settimeout(self.timeout)
                ackNum = self.receiveAck()
                # print('ack:\n '+str(ackNum))
                if ackNum == self.seqNum:
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
                self.resendTimes += 1
                if isinstance(e, socket.timeout):
                    timeout = True
                # print(str(self.seqNum))
                resendTimes += 1
                # print('resend %d at %d times' % (self.sendAckNum, resendTimes))
                # print('timeout '+str(self.timeout)+'sec')
                # Todo: 不理解这个if
                if resendTimes >= 5:
                    if not self.started:
                        ackFinish = True
                        return True
                    else:
                        self.started = False
                        raise Exception('resend times >= 5')
                self.sendto([packetDict[self.sendAckNum]])
                self.updataCongWin(True, timeout)
                self.updataTimeout(True)

        endTime = time.perf_counter()
        rtt = endTime - startTime
        self.updataCongWin(resendTimes != 0, timeout)
        self.updataTimeout(resendTimes != 0, rtt)
        return True

    def updataTimeout(self, resend, rtt=1):
        if resend == True:
            if self.timeout < self.maxTimeout:
                self.timeout *= 2
        else:
            self.timeout = 0.8*self.timeout+0.2*rtt+0.2*rtt

    def updataCongWin(self, resend, timeout):
        if resend == True:
            self.threshold = math.ceil(0.5*self.congWin)
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
        rawData, addr = self.recvfrom(200 + self.MSS)
        packet = RDTProtocol.parse(rawData)
        return packet.ackNum
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
     - Payload Length           0 - 512  (append zeros to the end if length < 512)
     - Sequence Number          0 - 2^16
     - Acknowledgement Number   0 - 2^16

    Checksum Algorithm:         16 bit one's complement of the one's complement sum

    Size of sender's window     1000
    """
    HEADER_LEN = 12
    MAX_PAYLOAD_LEN = 512
    SEGMENT_LEN = MAX_PAYLOAD_LEN + HEADER_LEN
    SEQ_NUM_BOUND = 2^16

    def __init__(self, seqNum: int, ackNum: int, checksum: int, payload: bytes, syn: bool = False, fin: bool = False, ack:bool = False):
        self.syn = syn
        self.fin = fin
        self.ack = ack
        self.seqNum = seqNum
        self.ackNum = ackNum
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
        arr[4] = checksum >> 8
        arr[5] = checksum & 0xFF
        arr.extend(b'\x00' * (RDTProtocol.SEGMENT_LEN - len(arr)))  # so that the total length is fixed
        return bytes(arr)

    @staticmethod
    def parse(segment: Union[bytes, bytearray]) -> 'RDTProtocol':
        """Parse raw bytes into an RDTSegment object"""
        try:
            assert len(segment) == RDTProtocol.SEGMENT_LEN
            # assert 0 <= len(segment) - 12 <= RDTProtocol.MAX_PAYLOAD_LEN
            assert calc_checksum(segment) == 0
            head, = struct.unpack('!H', segment[0:2])
            syn = (head & 0x8000) != 0
            fin = (head & 0x4000) != 0
            ack = (head & 0x2000) != 0
            length = head & 0x1FFF
            # assert length + 6 == len(segment)
            seq_num, ack_num, checksum = struct.unpack('!IIH', segment[2:10])
            payload = segment[10:10+length]
            return RDTProtocol(seq_num, ack_num, checksum, payload, syn, fin, ack)
        except AssertionError as e:
            raise ValueError from e


def calc_checksum(segment: Union[bytes, bytearray]) -> int:
    i = iter(segment)
    bytes_sum = sum(((a << 8) + b for a, b in zip(i, i)))  # for a, b: (s[0], s[1]), (s[2], s[3]), ...
    if len(segment) % 2 == 1:  # pad zeros to form a 16-bit word for checksum
        bytes_sum += segment[-1] << 8
    # add the overflow at the end (adding four times is sufficient)
    for i in range(3):
        bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
    return ~bytes_sum & 0xFFFF



