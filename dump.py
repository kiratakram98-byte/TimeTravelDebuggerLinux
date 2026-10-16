
import struct

d = open('session.tdbg','rb').read()

_, _, steps, idx = struct.unpack('<4siiq', d[:20])

offs = struct.unpack('<%dq' % steps, d[idx:idx+8*steps])

def i32(p): return struct.unpack('<i', d[p:p+4])[0]

def rs(p):

    n = i32(p); return d[p+4:p+4+n].decode(), p+4+n

def vs(p, n):

    out = []

    for _ in range(n):

        s, p = rs(p); out.append(f"{s}={i32(p)}"); p += 4

    return out, p

print("steps:", steps)

for k, o in enumerate(offs, 1):

    p = o; depth = i32(p); p += 4; fr = []

    for _ in range(depth):

        name, p = rs(p); argc = i32(p); p += 4

        a, p = vs(p, argc); p += 8

        lc = i32(p); p += 4

        l, p = vs(p, lc)

        fr.append(f"{name}(args:{','.join(a)} | locals:{','.join(l)})")

    print(k, ' > '.join(fr))

