import os
import hashlib
import asyncio
import binascii
import struct
import codecs
#import aiomysql
import base64
from bencodepy import encode, decode

import math
#from my_rdbs_cache import MysqlConsistent

class MessageType:
    REQUEST = 0
    DATA = 1
    REJECT = 2


BT_PROTOCOL = "BitTorrent protocol"
BT_PROTOCOL_LEN = len(BT_PROTOCOL)
EXT_ID = 20
EXT_HANDSHAKE_ID = 0
EXT_HANDSHAKE_MESSAGE = bytes([EXT_ID, EXT_HANDSHAKE_ID]) + encode({"m": {"ut_metadata": 1}})

BLOCK = math.pow(2, 14)
MAX_SIZE = BLOCK * 1000
BT_HEADER = b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x01'

META_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)),'metas')
def random_id(size=20):
    return os.urandom(size)


def get_ut_metadata(data):
    ut_metadata = b"ut_metadata"
    index = data.index(ut_metadata) + len(ut_metadata) + 1
    data = data[index:]
    return int(data[:data.index(b'e')])


def get_metadata_size(data):
    metadata_size = b"metadata_size"
    start = data.index(metadata_size) + len(metadata_size) + 1
    data = data[start:]
    return int(data[:data.index(b"e")])


class WirePeerClient:
    def __init__(self, infohash,file_name,data_q):
        if isinstance(infohash, str):
            infohash = binascii.unhexlify(infohash.upper())
        self.infohash = infohash
        self.peer_id = random_id()
        print('1111111111')

        self.writer = None
        self.reader = None

        self.ut_metadata = 0
        self.metadata_size = 0
        self.handshaked = False
        self.pieces_num = 0
        self.pieces_received_num = 0
        self.pieces = None

        #存储结果
        self.length = 0
        self.name = ""
        self.data = b""
        self.file_name = file_name
        #存储引擎
        # self.mysql_cache = mysql_cache
        self.data_q = data_q
        #标记是否有长度,如果没有发送总长度过来 那么长度就等于各个path之和
        self.has_length = True

    async def connect(self, ip, port, loop):
        self.reader, self.writer = await asyncio.open_connection(
            ip, port, loop=loop
        )
        print('82')

    def close(self):
        print(85)
        try:
            self.writer.close()
        except:
            pass

    def check_handshake(self, data):
        print(92)
        # Check BT Protocol Prefix
        if data[:20] != BT_HEADER[:20]:
            return False
        # Check InfoHash
        if data[28:48] != self.infohash:
            return False
        # Check support metadata exchange
        if data[25] != 16:
            return False
        return True

    def write_message(self, message):
        print(105)
        length = struct.pack(">I", len(message))
        self.writer.write(length + message)

    def request_piece(self, piece):
        msg = bytes([EXT_ID, self.ut_metadata]) + encode({"msg_type": 0, "piece": piece})
        self.write_message(msg)
    @asyncio.coroutine
    def pieces_complete(self):
        print(113)
        metainfo = b''.join(self.pieces)
        if len(metainfo) != self.metadata_size:
            # Wrong size
            print('size error')
            return self.close()

        infohash = hashlib.sha1(metainfo).hexdigest()
        if binascii.unhexlify(infohash.upper()) != self.infohash:
            # Wrong infohash
            print('hashinfo error')
            return self.close()

        b_data = base64.b64encode(metainfo)
        data = b_data.decode()
        hex_info_hash = codecs.getencoder("hex")(self.infohash)[0].decode()
        print(data,'---------------')
        yield from self.data_q.put((hex_info_hash,data))

        #先把meta全部数据存储到数据库中
        #TODO 数据存储在redis中,然后从redis中去取数据,持久化到monogo或者mysql最后放入es中
        #TODO 数据存储在做过滤
        # try:
        #     for i in data:
        #         if i == b'length':
        #             self.length = data[i]
        #         else:
        #             self.has_length = False
        #         if i == b'name':
        #             try:
        #                 self.name = data[i].decode()
        #             except Exception:
        #                 self.name = data[i]
        #         if i == b'files':
        #             files = data[i]
        #             for i in files:
        #                 if b'length' in i:
        #                     length = i[b'length']
        #                     if not self.has_length:
        #                         self.length += length
        #                 if b'path' in i:
        #                     paths = i[b'path']
        #                     for j in paths:
        #                         name = j.decode()
        #
        # except Exception as e:
        #     print(e)
        # if self.name:
        #     """
        #     持久化数据到Mysql中
        #     """
        #     yield from self.data_q.put((hex_info_hash,self.length,self.name))
        #     print('data_q-->put')


    async def work(self):

        self.writer.write(BT_HEADER + self.infohash + self.peer_id)
        print(172)
        while True:
            if not self.handshaked:
                print(175)
                if self.check_handshake(await self.reader.readexactly(68)):
                    self.handshaked = True
                    # Send EXT Handshake
                    self.write_message(EXT_HANDSHAKE_MESSAGE)
                else:
                    return self.close()

            total_message_length, msg_id = struct.unpack("!IB", await self.reader.readexactly(5))
            # Total message length contains message id length, remove it
            payload_length = total_message_length - 1
            payload = await self.reader.readexactly(payload_length)

            if msg_id != EXT_ID:
                continue
            extended_id, extend_payload = payload[0], payload[1:]
            if extended_id == 0 and not self.ut_metadata:
                # Extend handshake, receive ut_metadata and metadata_size
                try:
                    self.ut_metadata = get_ut_metadata(extend_payload)
                    self.metadata_size = get_metadata_size(extend_payload)
                except:
                    return self.close()
                self.pieces_num = math.ceil(self.metadata_size / BLOCK)
                self.pieces = [False] * self.pieces_num
                self.request_piece(0)
                continue

            try:
                split_index = extend_payload.index(b"ee") + 2
                info = decode(extend_payload[:split_index])
                if info[b'msg_type'] != MessageType.DATA:
                    return self.close()
                if info[b'piece'] != self.pieces_received_num:
                    return self.close()
                self.pieces[info[b'piece']] = extend_payload[split_index:]
            except:
                return self.close()
            self.pieces_received_num += 1

            if self.pieces_received_num == self.pieces_num:
                # return self.pieces_complete()
                loop = asyncio.get_event_loop()
                loop.create_task(self.pieces_complete())
            else:
                self.request_piece(self.pieces_received_num)


async def get_metadata(infohash, ip, port, loop=None,filename=1,data_q=None):
    if not loop:
        loop = asyncio.get_event_loop()

    client = WirePeerClient(infohash,filename,data_q=data_q)
    try:
        await client.connect(ip, port, loop)
        return await client.work()
    except Exception as e:
        client.close()
        return False
