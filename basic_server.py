import socket
from config import BOOTSTRAP_NODES
from bencodepy import encode, decode
import os
import sys
import queue

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import selectors

sys.path.append(BASE_DIR)
from handler.utils import get_logger, get_nodes_info, get_rand_id, get_neighbor
from sel.poll import PollIO, IO_WRITE, IO_READ
import asyncio

que = asyncio.queues.Queue()


class HDTServer():
    UDP_RECV_BUFFSIZE = 65535

    def __init__(self, bind_ip='0.0.0.0', bind_port=19527):
        super().__init__()
        # asyncore.dispatcher.__init__(self)

        # self.create_socket(socket.AF_INET,socket.SOCK_DGRAM)
        # self.set_reuse_addr()

        self.ip = bind_ip
        self.port = bind_port
        # self.udp = socket.socket(
        #     socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        # )
        #
        # self.udp.bind((self.ip, self.port))

        self.nid = get_rand_id()

        self.queue = que

        # self.sel = selectors.DefaultSelector()
        # self.bootstrap()
        #
        # #self.sel.register(self.udp, selectors.EVENT_READ | selectors.EVENT_WRITE, self.acc)
        # event = selectors.EVENT_WRITE | selectors.EVENT_READ
        # self.sel.register(self.udp, event, self.send)

    def bootstrap(self):
        for node_address in BOOTSTRAP_NODES:
            self.find_node_msg(node_address)

    # def writable(self):
    #     msg,addr = self.queue.get()

    def find_node_msg(self, address, nid=None):
        """
        封装请求查找node的请求报文
        :param address:
        :param nid:
        :return:
        """
        nid = get_neighbor(nid) if nid else self.nid
        tid = get_rand_id()
        msg = dict(
            t=tid,
            y="q",  # 表示请求的意思
            q="find_node",  # 指定请求为 find_node
            a=dict(id=nid, target=get_rand_id()),
        )
        self.send_krpc_msg(msg, address)

    async def send_krpc_msg(self, msg, address):
        """
        把send消息放入队列中
        :param msg:
        :param address:
        :return:
        """
        try:
            # msg 要经过 bencode 编码
            msg = encode(msg)
            # self.udp.sendto(msg, address)
            send_msg = (msg, address)
            await self.queue.put(send_msg)
        except Exception as e:
            print(e)

    # def send_krpc(self, msg, address):
    #     """
    #     发送 krpc 协议
    #
    #     :param msg: 发送 UDP 报文信息
    #     :param address: 发送地址，(ip, port) 元组
    #     """
    #     try:
    #         # msg 要经过 bencode 编码
    #         msg = encode(msg)
    #         self.udp.sendto(msg, address)
    #     except Exception as e:
    #         print(e)

    # def connection_made(self, transport):
    #     peername = transport.get_extra_info('peername')
    #     print('Connection from {}'.format(peername))
    #     self.transport = transport

    #     self.bootstrap()
    #     server = Handler(self.ip,self.port)
    #     asyncore.loop()
    # self.sel.register_event(self.udp.fileno(),IO_WRITE)
    # # self.sel.register_event(self.udp.fileno(),IO_READ)
    # print(2222)
    # while True:
    #     # 接受返回报文
    #     # data, address = self.udp.recvfrom(self.UDP_RECV_BUFFSIZE)
    #     # # 使用 bdecode 解码返回数据
    #     # msg = decode(data)
    #     # print(2222)
    #     # # 处理返回信息
    #     # print(msg)
    #     for fd,ev in self.sel.wait_events(1):
    #         print(3333)
    #         if ev & IO_WRITE:
    #             if fd == self.udp.fileno():
    #                 msg,addr = self.queue.get_nowait()
    #                 self.udp.send(msg,addr)
    #                 print(1111)
    #                 #self.sel.unregister_event(fd)
    #                 #self.sel.register_event(fd,IO_WRITE)
    #         if ev & IO_READ:
    #             if fd == self.udp.fileno():
    #                 msg,addr = self.udp.recvfrom(self.UDP_RECV_BUFFSIZE)
    #                 print(msg)

    # self.on_message(msg, address)
    # time.sleep(SLEEP_TIME)
    # except Exception as e:
    #     self.logger.warning(e)


import asyncio


class EchoServerProtocol:
    UDP_RECV_BUFFSIZE = 65535

    def __init__(self, bind_ip='0.0.0.0', bind_port=19527):
        super().__init__()
        self.ip = bind_ip
        self.port = bind_port
        self.nid = get_rand_id()
        self.queue = asyncio.queues.Queue()
        self.bootstrap()

    def bootstrap(self):
        for node_address in BOOTSTRAP_NODES:
            self.find_node_msg(node_address)

    def connection_made(self, transport):
        self.transport = transport
        msg, addr = self.queue.get()
        self.transport.sendto(msg, addr)

    def datagram_received(self, data, addr):
        data = (data,addr)
        self.handle_data(data)

    def handle_data(self,data):
        print(data)

    def find_node_msg(self, address, nid=None):
        """
        封装请求查找node的请求报文
        :param address:
        :param nid:
        :return:
        """
        nid = get_neighbor(nid) if nid else self.nid
        tid = get_rand_id()
        msg = dict(
            t=tid,
            y="q",  # 表示请求的意思
            q="find_node",  # 指定请求为 find_node
            a=dict(id=nid, target=get_rand_id()),
        )
        msg = self.krpc_msg(msg)
        if msg:
            data = (msg,address)
            self.send_msg_to_queue(data)

    def krpc_msg(self,msg):
        try:
            msg = encode(msg)
            return msg
        except Exception:
            return False

    def send_msg_to_queue(self,msg):
        try:
            self.queue.put(msg)
        except Exception:
            print('msg in queue error')


loop = asyncio.get_event_loop()
print("Starting UDP server")
# One protocol instance will be created to serve all client requests
listen = loop.create_datagram_endpoint(
    EchoServerProtocol, local_addr=('0.0.0.0', 9999))
transport, protocol = loop.run_until_complete(listen)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

transport.close()
loop.close()
