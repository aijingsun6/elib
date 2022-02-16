elib
=====
erlang normal lib module


### 1. mysql_proxy
```
方法1：
Opt = [{host, "localhost"}, {user, "foo"}, {password, "hello"}, {database, "test"}].
mysql_proxy:start_link(Name, Opt).

方法2：
{elib,
      [
          {mysql,[
                  {host,"${MYSQL_HOST}"},
                  {port,${MYSQL_PORT}},
                  {user,"${MYSQL_USER}"},
                  {password,"${MYSQL_PASS}"},
                  {database,"${MYSQL_DB}"},
                  {size,${MYSQL_SIZE}}
          ]}
      ]
  }

```

### 2. redis_proxy
```
方法1：
Opt = [{host,"127.0.0.1"}, {port,6379}, {database,0}, {username,""},{password,"123456"},{reconnect_sleep,100},{connect_timeout,5000}].
redis_proxy:start_link(Name,Opt).
方法2：
{elib,
      [
          {redis,[
                            {host,"${REDIS_HOST}"},
                            {port,${REDIS_PORT}},
                            {username,"${REDIS_USER}"},
                            {password,"${REDIS_PASS}"},
                            {database,${REDIS_DB}},
                            {size,${REDIS_SIZE}}
                    ]}

      ]
  }
```

### 3. redis_global
使用redis替换global模块
```
方法1：
redis_global:start_link().

方法2：
{elib,
      [
          {redis_global,true}
      ]
  }
```

### 4. ndisc_redis
使用redis作为节点发现服务
```
方法1：
ndisc_redis:start_link(Cluster).

方法2：
{elib,
      [
          {cluster_id,<<"cluster_id">>}
      ]
  }
```
