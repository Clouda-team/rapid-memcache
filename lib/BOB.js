exports.serialize = function (data) {
    var capacity = 512,
        buf = new Buffer(capacity),
        pos = 0;
    var objects = [], strings = [];
    walk(data);
    return buf.slice(0, pos);
    function walk(data) {
        switch (typeof data) {
            case 'undefined':
                append(0);
                break;
            case 'string':
                addString(data);
                break;
            case 'number':
                ensureCapacity(9);
                buf[pos++] = 0x44; // D
                buf.writeDoubleBE(data, pos, true);
                pos += 8;
                break;
            case 'boolean':
                append(data ? 0x54 : 0x46); // T:F
                break;
            default :
                if (data === null) {
                    append(0x30);// 0
                    break;
                }
                var idx = objects.indexOf(data);
                if (~idx) { // not -1
                    writeInt(0x4F, idx); // O
                    break;
                }
                objects.push(data);
                if (data instanceof Array) {
                    var next = 0;
                    if (data.some(function (value, i) {
                        if (i !== next) { // rare array
                            return true;
                        }
                        next++;
                    })) {
                        writeInt(0x2E, data.length); // .
                        var marker = 0;
                        data.forEach(function (value, i) {
                            marker = pos;
                            writeInt(1, i);
                            walk(value);
                        });
                        buf[marker] = 0;
                    } else {
                        writeInt(0x5B, data.length); // [
                        data.forEach(walk);
                    }
                } else {
                    var keys = Object.keys(data);
                    writeInt(0x7B, keys.length); // {
                    keys.forEach(function (key) {
                        addString(key);
                        walk(data[key]);
                    });
                }
                objects.push(data);
                break;
        }
    }

    function addString(data) {
        if (data.length > 2) {
            var idx = strings.indexOf(data);
            if (~idx) { // found
                writeInt(0x53, idx); // S
                return;
            }
            strings.push(data);
        }
        var buffer = new Buffer(data, 'utf16le'), len = buffer.length;
        ensureCapacity(5 + len);
        writeInt(0x24, len); // $
        buffer.copy(buf, pos);
        pos += buffer.length;
    }

    function writeInt(type, n) {
        ensureCapacity(5);
        buf[pos++] = type;
        buf.writeUInt32BE(n, pos, true);
        pos += 4;
    }

    function ensureCapacity(n) {
        if (pos + n > capacity) {
            do {
                capacity <<= 1;
            } while (pos + n > capacity);
            var newBuf = new Buffer(capacity);
            buf.copy(newBuf, 0, 0, pos);
            buf = newBuf;
        }
    }

    function append(type) {
        ensureCapacity(1);
        buf[pos++] = type;
    }

};
exports.parse = function (buf) {
    var pos = 0,
        objects = [],
        strings = [];
    return read();
    function read() {
        switch (buf[pos++]) {
            case 0: // undefined
                return;
            case 0x53: // S
                return strings[getInt()];
            case 0x24: // $
                var len = getInt(),
                    start = pos;
                var ret = buf.slice(start, pos += len).toString('utf16le');
                if (ret.length > 2)strings.push(ret);
                return ret;
            case 0x44: // D
                ret = buf.readDoubleBE(pos);
                pos += 8;
                return ret;
            case 0x54:
                return true;
            case 0x46:
                return false;
            case 0x30:
                return null;
            case 0x4F: // O
                return objects[getInt()];
            case 0x5B: // [
                len = getInt();
                ret = Array(len);
                objects.push(ret);
                for (var i = 0; i < len; i++) {
                    ret[i] = read();
                }
                return ret;
            case 0x7B:// {
                len = getInt();
                ret = {};
                objects.push(ret);
                for (var i = 0; i < len; i++) {
                    ret[read()] = read();
                }
                return ret;
            case 0x2E: // .
                len = getInt();
                ret = Array(len);
                objects.push(ret);
                for (; ;) {
                    var next = buf[pos++];
                    var i = getInt();
                    ret[i] = read();
                    if (!next)break;
                }
                return ret;

        }
    }

    function getInt() {
        var ret = buf.readUInt32BE(pos, true);
        pos += 4;
        return ret;
    }
};