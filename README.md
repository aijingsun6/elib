elib
=====
erlang normal lib module


### 1. mysql_proxy
```
Opt = [{host, "localhost"}, {user, "foo"}, {password, "hello"}, {database, "test"}].

mysql_proxy:start_link(mysql_proxy, Opt).
```

### 2. redis_proxy
```
Opt = [
{host,"127.0.0.1"}, {port,6379}, {database,0}, 
{username,""},{password,"123456"},{reconnect_sleep,100},
{connect_timeout,5000}].

redis_proxy:start_link(redis_proxy,Opt).
```

### 3. redis_global
使用redis替换global模块
```
-module(redis_global_example).

start_link(Name) ->
  SName = redis_global:svr_name(Name),
  gen_server:start_link(SName, ?MODULE, [Name], []).

```
