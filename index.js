var net = require('net'),
    util = require('util'),
    q = require('q'),
    BOB = require('./lib/BOB'),
    zlib = require('zlib');

exports.instance = function (options) {
    return new Memcached(options);
};

function buildRequest(op, key, data, extra) {
    var bodyLen = (extra ? extra.length : 0) + (key ? key.length : 0) + (data ? data.length : 0);
    var buf = new Buffer(bodyLen + 24);
    buf.writeUInt16BE(0x8000 | op, 0, true);
    buf.writeUInt16BE(key ? key.length : 0, 2, true);
    buf.writeUInt32BE(0, 4, true);
    if (extra) {
        buf[4] = extra.length;
    }
    buf.writeUInt32BE(bodyLen, 8, true);
    buf.writeUInt32BE(0, 12, true);
    buf.writeUInt32BE(0, 16, true);
    buf.writeUInt32BE(0, 20, true);
    var pos = 24;
    if (extra) {
        extra.copy(buf, 24);
        pos += extra.length;
    }
    if (key) {
        buf.write(key, pos, key.length, 'binary');
        pos += key.length;
    }
    if (data) {
        data.copy(buf, pos);
    }
    return buf;
}

var binaryConnection = function (options) {
    var pending = 0, buffers = null, requestIds = 0, callbacks = [];
    var ret = net.connect(options);
    ret.on('data', onBuffer);
    ret.request = function (buffer, onerr, cb) {
        var id = requestIds++;
        buffer.writeUInt32BE(id, 12, true);
        callbacks.push([id, onerr, cb]);
//        console.log('SEND#' + id, buffer.length);
        ret.write(buffer);
    };
    return ret;
    function onBuffer(buffer) {
        var len = buffer.length;
//        console.log('RECV', len, pending, buffer);
        if (pending) {
            if (pending > len) {
                buffers.push(buffer);
                pending -= len;
            } else if (pending === len) {
                buffers.push(buffer);
                buffer = Buffer.concat(buffers);
                pending = 0;
                buffers = null;
                onResponse(buffer);
            } else {
                buffers.push(buffer.slice(0, pending));
                buffer = buffer.slice(pending);
                pending = 0;
                onResponse(Buffer.concat(buffers));
                buffers = null;
                onBuffer(buffer);
                ret.emit('error', new Error('ERRPACKETLEN'));
            }
        } else {
            if (buffer[0] !== 0x81) {// bad magic
                return ret.emit('error', new Error('ERRMAGIC'));
            }
            var totalLen = buffer.readUInt32BE(8) + 24;
            if (len === totalLen) {
                onResponse(buffer);
            } else if (len < totalLen) {
                buffers = [buffer];
                pending = totalLen - len;
            } else {
                onResponse(buffer.slice(0, totalLen));
                onBuffer(buffer.slice(totalLen));
            }
        }
    }

    function onResponse(buffer) {
        var keyLen = buffer.readUInt16BE(2), extraLen = buffer[4];
        var id = buffer.readUInt32BE(12), entry;
//        console.log('RECV#' + id, buffer.length);
        while (entry = callbacks.shift()) {
            if (entry[0] === id) { // found
                break;
            }
            entry[1](new Error('ERRSEQ'));
        }
        if (entry) { // found
            entry[2]({
                op: buffer[1],
                status: buffer.readUInt16BE(6),
                extra: extraLen ? buffer.slice(24, 24 + extraLen) : null,
                key: buffer.slice(24 + extraLen, 24 + extraLen + keyLen).toString(),
                data: buffer.slice(24 + extraLen + keyLen)
            });
        }
    }
};

function Memcached(options) {
    if (!options.socketPath) {
        if (!options.host) {
            options.host = '127.0.0.1';
        }
        if (!options.port) {
            options.port = 11211;
        }
    }
    var conf = this._conf = util._extend({}, this._conf);

    for (var keys = Object.keys(conf), n = keys.length; n--;) {
        var key = keys[n];
        if (options.hasOwnProperty(key)) {
            conf[key] = options[key];
        }
    }
    var clusters = conf.clusters, N;
    if (clusters) {
        N = clusters.length;
        clusters.forEach(function (obj) {
            obj.__proto__ = options;
            var forbidCount = obj.forbidCount || 10;
            obj.forbidCount = N === 1 ? 0 : forbidCount / (N - 1);
            obj.forbidden = 0;
        });
    } else {
        options.forbidden = options.forbidCount = 0;
        N = 1;
    }


    var conns = [], // all free connections
        allowedAgents = conf.maxConnects,
        pending = [], keepAliveTimer = null, connects = 0;
    var authData = null;
    if (conf.user) {
        authData = buildRequest(0x21, 'PLAIN', new Buffer('\0' + conf.user + '\0' + conf.password));
    }

    this._context = {
        getConnection: function (cb) {
            var L = conns.length;
            if (L) {
                if (L === 1) {
                    clearTimeout(keepAliveTimer);
                    keepAliveTimer = null;
                }
                return cb(null, conns.pop());
            }
            pending.push(cb);
            if (allowedAgents) {
                allowedAgents--;
                connect(conf.maxRetries);
            }
        },
        releaseConnection: release,
        request: function (buf) {
            var deferred = q.defer();
            this.getConnection(function (err, conn) {
                if (err) {
                    return deferred.reject(err);
                }
                conn.request(buf, deferred.reject, function (response) {
                    deferred.resolve(response);
                    release(conn);
                });
            });
            return deferred.promise;
        }
    };

    // 申请新的连接
    function connect(retries) {
        allowedAgents--;
        var option;
        if (clusters) {
            for (; ;) {
                option = clusters[connects++ % N];
                if (option.forbidden) {
                    option.forbidden--;
                } else {
                    break;
                }
            }
        } else {
            option = options;
        }
        var conn = binaryConnection(option);
        conn.once('error', connectfail);
        if (authData) {
            conn.request(authData, function (err) { // error
                end(conn);
                failAll(err);
            }, function (res) {
                if (res.status) {// bad response, do not retry
                    end(conn);
                    failAll(new Error(res.data.toString()));
                } else {
                    conn.removeListener('error', connectfail);
                    conn.on('error', onerror);
                    release(conn);
                }
            });
        } else {
            conn.once('connect', function () {
                release(conn);
            });
        }

        function connectfail(err) {
            option.forbidden = option.forbidCount;
            if (retries) {
                return setTimeout(connect, clusters ? 0 : conf.retryTimeout, retries - 1);
            }
            failAll(err);
        }

        function failAll(err) {
            pending.forEach(function (cb) {
                cb(err);
            });
        }

//        conn.connect(function (err) {
//            if (err) {
////                console.log('connect::' + err.message, retries);
//                if (typeof err.code !== 'number') {
//                    option.forbidden = option.forbidCount;
//                    if (retries) {
//                        return setTimeout(connect, clusters ? 0 : conf.retryTimeout, retries - 1);
//                    }
//                }
//                // report error to all pending responses
//                pending.forEach(function (cb) {
//                    cb(err);
//                });
//                pending.length = 0;
//                allowedAgents++;
//            } else { // connected
//                conn.expires = Date.now() + conf.keepAliveMaxLife;
//                release(conn);
//            }
//        });
    }

    function release(conn) {
        var t = Date.now();
        if (t > conn.expires) { // connection expired
            end(conn);
        }
        if (pending.length) {
            pending.pop()(null, conn);
        } else {
            conns.push(conn);
            if (conns.length === 1) {
                keepAliveTimer = setTimeout(keepAliveTimeout, conf.keepAliveTimeout);
//                console.log('conn will expire in', conf.keepAliveTimeout);
            } else {
                conn.keepAliveExpires = t + conf.keepAliveTimeout;
            }
        }
    }

    function end(conn) {
        allowedAgents++;
        try {
            conn.end(nop);
        } catch (e) {
        }
    }

    function keepAliveTimeout() {
        var conn = conns.shift();
        end(conn);
        keepAliveTimer = conns.length ? setTimeout(keepAliveTimeout, conns[0].keepAliveExpires - Date.now()) : null;
    }

    function nop() {
    }

    function onerror(err) {
        if (this.defer) {
            this.defer.reject(err);
        } else { // find and remove from pending list
            for (var i = 0, L = conns.length; i < L; i++) {
                if (conns[i] === this) {
                    conns.splice(i, 1);
                    break;
                }
            }
        }
        end(this);
    }

}


Memcached.prototype = {
    impl: {storage: true},
    _conf: {
        user: null,
        password: null,
        clusters: null,
        maxConnects: 30,
        retryTimeout: 400,
        maxRetries: 3,
        keepAliveTimeout: 5000,
        keepAliveMaxLife: 300000
    },
    _context: null,
    set: function (key, val, options) {
        var flags, buf;
        switch (typeof val) {
            case 'string':
                flags = 0;
                buf = new Buffer(val);
                break;
            case 'number':
                flags = 0x44424c45; // DBLE
                buf = new Buffer(8);
                buf.writeDoubleBE(val, 0);
                break;
            case 'boolean':
                flags = val ? 0x54525545 : 0x46414c53; // TRUE : FALS
                buf = new Buffer(0);
                break;
            default:
                if (val) {
                    flags = 0x42534F4E; // BSON
                    buf = BOB.serialize(val);
                } else { // null or undefined
                    flags = 0x4E554C4C; // NULL
                    buf = new Buffer(0);
                }
                break;
        }
        var expires = options && options.expires ? options.expires < 2000 ? 1 : options.expires / 2000 | 0 : 0;
        var self = this;
        if (buf.length > 65535) { //
            // try compress
            return q.nfcall(zlib.deflateRaw, buf).then(function (compressed) {
                flags |= 0x80000000;
                buf = compressed;
                return send();
            });
        } else {
            return send();
        }
        function send() {
            var extra = new Buffer(8);
            extra.writeUInt32BE(flags, 0, true);
            extra.writeUInt32BE(expires, 4, true);
            return self._context.request(buildRequest(1, key, buf, extra)).then(function (res) {
                if (res.status) {
                    throw new Error(res.data);
                }
            });
        }
    },
    get: function (key) {
        return this._context.request(buildRequest(0, key)).then(function (res) {
            if (res.status) {
                if (res.status === 1) { // key not found
                    return;
                }
                throw new Error(res.data);
            }
            var flag = res.extra.readUInt32BE(0), buf = res.data;
            if (flag & 0x80000000) { // compressed
                return q.nfcall(zlib.inflateRaw, buf).then(function (inflated) {
                    flag ^= 0x80000000;
                    buf = inflated;
                    return parse();
                });
            } else {
                return parse();
            }
            function parse() {

                switch (flag) {
                    case 0://string
                        return buf.toString();
                    case 1:
                        return inflate().then(function (buf) {
                            return buf.toString();
                        });
                    case 0x44424c45://DBLE
                        return buf.readDoubleBE(0);
                    case 0x54525545: // TRUE
                        return true;
                    case 0x46414c53: //FALS
                        return false;
                    case 0x4E554C4C:
                        return null;
                    case 0x42534F4E:
                        return BOB.parse(buf);
                }
            }
        });
    },
    delete: function (key) {
        return this._context.request(buildRequest(4, key)).then(function (res) {
            if (res.status) {
                if (res.status === 1) { // key not found
                    return;
                }
                throw new Error(res.data);
            }
        });
    }
};