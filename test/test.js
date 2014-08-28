var assert = require('assert');

var cache = require('../').instance({
    user: "3c0cd9292dbf11e4",
    password: 'Good_Job'
});

describe('basic', function () {
    var data = 'bar';
    it('set', function (next) {
        cache.set('foo', data).then(function () {
            next();
        }).done();
    });
    it('get', function (next) {
        cache.get('foo').then(function (ret) {
            assert.strictEqual(ret, data);
            next();
        }).done();
    });
    it('get not found', function (next) {
        cache.get('not_exist').then(function (ret) {
            assert.strictEqual(ret, undefined);
            next();
        }).done();
    });
});


describe('data types', function () {
    it('number', function (next) {
        var data = Math.random();
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert.strictEqual(ret, data);
            next();
        }).done();
    });
    it('true', function (next) {
        var data = true;
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert.strictEqual(ret, data);
            next();
        }).done();
    });
    it('false', function (next) {
        var data = false;
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert.strictEqual(ret, data);
            next();
        }).done();
    });
    it('null', function (next) {
        var data = null;
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert.strictEqual(ret, data);
            next();
        }).done();
    });
    it('array', function (next) {
        var data = [1, Math.random(), Date.now(), {}, 'foo', null];
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert.deepEqual(ret, data);
            next();
        }).done();
    });
    it('array', function (next) {
        var data = [1, Math.random(), Date.now(), {}];
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert.deepEqual(ret, data);
            next();
        }).done();
    });
    it('circular object', function (next) {
        var data = [, Math.random(), {}, {foo: []}, 'foo'];
        data[0] = data;
        data[2].test = data;
        data[3].foo.push(data);
        cache.set('key', data).then(function () {
            return cache.get('key');
        }).then(function (ret) {
            assert(ret === ret[0]);
            assert.strictEqual(ret[1], data[1]);
            assert(ret === ret[2].test);
            assert(ret[3].foo[0] === ret);
            next();
        }).done();
    });


});

describe('data compression', function () {
    this.timeout(2000);
    it('rare array', function (next) {
        var data = [];
        data[0] = 123;
        data[99] = 456;
        data[9999999] = 789;
        cache.set('rareArray', data).then(function () {
            return cache.get('rareArray');
        }).then(function (ret) {
            assert(ret.length === 1e7);
            assert(ret[0] === 123);
            assert(ret[99] === 456);
            assert(ret[9999999] === 789);
            next();
        }).done();
    });

    it('loooong string', function (next) {
        var data = 'Lorem ipsum dolor sit amet;';
        for (var i = 0; i < 20; i++) data += data;
        cache.set('longString', data).then(function () {
            return cache.get('longString');
        }).then(function (ret) {
            assert(ret === data);
            next();
        }).done();
    });
    it('loooong array', function (next) {
        var data = [];
        for (var i = 0; i < 1e5; i++) {
            data[i] = 0;
        }
        cache.set('longArray', data).then(function () {
            return cache.get('longArray');
        }).then(function (ret) {
            assert.deepEqual(ret, data);
            next();
        }).done();
    });
    it('repeat string', function (next) {
        var data = 'Lorem ipsum dolor sit amet;';
        for (var i = 0; i < 10; i++) data += data;
        var arr = [];
        for (var i = 0; i < 1e4; i++) {
            arr[i] = data;
        }
        cache.set('repeatString', arr).then(function () {
            return cache.get('repeatString');
        }).then(function (ret) {
            assert.deepEqual(ret, arr);
            next();
        }).done();
    });
    it('global', function (next) {
        cache.set('global', global).then(function () {
            return cache.get('global');
        }).then(function (ret) {
            assert(ret === ret.global);
            assert.deepEqual(ret.process.env, process.env);
            assert.deepEqual(ret.process.versions, process.versions);
            next();
        }).done();
    });
});

describe('delete', function () {
    it('delete', function (next) {
        cache.delete('global').then(function () {
            return cache.get('global');
        }).then(function (ret) {
            assert(ret === undefined);
            next();
        }).done();
    });
});

describe('expires', function () {
    this.timeout(3000);
    it('set with expires', function (next) {
        cache.set('will_expire', 'bar', {expires: 2000}).then(function () {
            setTimeout(function () {
                cache.get('will_expire').then(function (ret) {
                    assert.strictEqual(ret, undefined);
                    next();
                }).done();
            }, 2200);
        }).done();
    });
});
