#!/usr/bin/env python3
"""NexusDB smoke 客户端: put/get roundtrip.

帧格式: [total u32][req_id u64][op u8][key_len u16][val_len u32][key][val]

用法:
  smoke_client.py           # put k1 + get k1 验证
  smoke_client.py --verify  # 只 get k1 验证 (用于重启后持久化检查)
"""
import socket, struct, sys

addr = ("127.0.0.1", 15433)
HEADER = 19

def frame(req_id, op, key, val=b""):
    total = HEADER + len(key) + len(val)
    return struct.pack(">IQBHI", total, req_id, op, len(key), len(val)) + key + val

def parse(resp):
    total, rid, op, klen, vlen = struct.unpack(">IQBHI", resp[:HEADER])
    val = resp[HEADER + klen : HEADER + klen + vlen]
    return rid, op, val

verify_only = "--verify" in sys.argv

s = socket.create_connection(addr, timeout=5)
s.settimeout(5)

if not verify_only:
    # put k1=v_hello
    s.sendall(frame(1, 1, b"k1", b"v_hello"))
    rid, op, _ = parse(s.recv(4096))
    assert rid == 1 and op == 0x10, f"put resp: rid={rid} op={hex(op)}"

# get k1
s.sendall(frame(2, 2, b"k1"))
rid, op, val = parse(s.recv(4096))
assert rid == 2 and op == 0x11 and val == b"v_hello", f"get resp: rid={rid} op={hex(op)} val={val!r}"
print("SMOKE_OK" + ("_VERIFY" if verify_only else "") + " value =", val.decode())
s.close()
