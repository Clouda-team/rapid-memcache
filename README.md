RAPID-MEMCACHE
====

node.js Memcached 客户端
 
###安装

    npm install rapid-memcache

###使用

    var cache = require('../').instance({
        host: '10.0.0.222', // default is '127.0.0.1'
        port: 11211, // default is 11211
        user: "username",
        password: 'password'
    });
    cache.set('foo', 'bar').then(function(){
        cache.get('foo').then(function(ret){
            ...
        });
    });

更多API详情参考 [clouda+](http://cloudaplus.duapp.com/) 