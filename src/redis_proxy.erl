-module(redis_proxy).
-include_lib("kernel/include/logger.hrl").
%% API
-export([
  start_link/1,
  start_link/2,
  stop/0,
  stop/1,
  status/0,
  status/1
]).

-export([
  q/1,
  q/2,
  q/3,
  qp/1,
  qp/2,
  qp/3,
  foreach_redis/4,
  foreach_redis/5
]).

%% 利用redis自旋锁
-export([
  trans/2,
  trans/3,
  trans/5
]).

-export([
  del/1, del/3,
  exists/1, exists/3,
  expire/2, expire/4,
  ttl/1, ttl/3,
  type/1, type/3
]).

-export([
  append/2, append/4,
  decr/1, decr/3,
  decrby/2, decrby/4,
  get/1, get/3,
  get_del/1, get_del/3,
  get_ex/2, get_ex/4,
  get_range/3, get_range/5,
  incr/1, incr/3,
  incrby/2, incrby/4,
  mget/1, mget/3,
  mset/1, mset/3,
  mset_nx/1, mset_nx/3,
  set/3, set/5,
  set_ex/3, set_ex/5,
  set_nx/2, set_nx/4,
  set_range/3, set_range/5
]).

-define(SERVER, ?MODULE).
-define(DEFAULT_POOL_SIZE, 16).
-define(TIMEOUT, 5000).
-define(REDIS_LOCK_TIME, 5000).
-define(REDIS_TRANS_TIMEOUT, infinity).
-define(REDIS_TIMEOUT, 5000).
-define(TRANS_SLEEP_TIME_MAX, 100).
-define(BOOL_REPLY_FUN, fun(<<"OK">>) -> true;(_) -> false end).
-define(INTEGER_REPLY_FUN, fun(X) -> erlang:binary_to_integer(X) end).
-define(REPLY_FUN, fun(X) -> X end).

start_link(Args) ->
  L = parse_config(Args),
  poolboy:start_link(L).

start_link(Name, Args) when is_atom(Name) ->
  L = parse_config(Args),
  L2 = [{name, {local, Name}} | L],
  poolboy:start_link(L2).

parse_config(M) when is_map(M) ->
  Config = #{worker_module => redis_worker, size => ?DEFAULT_POOL_SIZE},
  M2 = maps:merge(Config, M),
  maps:to_list(M2);
parse_config(L) when is_list(L) ->
  parse_config(maps:from_list(L)).


stop() ->
  stop(?SERVER).

stop(Name) when is_atom(Name) ->
  poolboy:stop(Name).

status() ->
  status(?SERVER).

status(Name) when is_atom(Name) ->
  poolboy:status(Name).

q(Command) -> q(Command, ?TIMEOUT).

q(Command, Timeout) -> q(?SERVER, Command, Timeout).

q(Name, Command, Timeout) ->
  poolboy:transaction(Name, fun(PID) -> eredis:q(PID, Command) end, Timeout).

qp(Command) -> qp(Command, ?TIMEOUT).

qp(Command, Timeout) -> qp(?SERVER, Command, Timeout).

qp(Name, Command, Timeout) ->
  poolboy:transaction(Name, fun(PID) -> eredis:qp(PID, Command) end, Timeout).

foreach_redis(REG, Fun, CntPerTime, Timeout) ->
  foreach_redis(?SERVER, REG, Fun, CntPerTime, Timeout).

foreach_redis(Name, REG, Fun, CntPerTime, Timeout) ->
  foreach_redis(Name, <<"0">>, 0, REG, CntPerTime, Timeout, Fun).

foreach_redis(_Name, <<"0">>, Times, _REG, _CNT, _Timeout, _Fun) when Times > 0 ->
  ok;
foreach_redis(Name, Cur, Times, REG, CNT, Timeout, Fun) ->
  CMD = case REG == "*" of
          true -> ["SCAN", Cur, "COUNT", erlang:integer_to_list(CNT)];
          false -> ["SCAN", Cur, "MATCH", REG, "COUNT", erlang:integer_to_list(CNT)]
        end,
  case q(Name, CMD, Timeout) of
    {ok, [Cur2, []]} ->
      foreach_redis(Name, Cur2, Times + 1, REG, CNT, Timeout, Fun);
    {ok, [Cur2, L]} ->
      case Fun of
        {M, F} -> M:F(L);
        _ -> Fun(L)
      end,
      foreach_redis(Name, Cur2, Times + 1, REG, CNT, Timeout, Fun)
  end.

%% ====================
%% Fun = {M,F,A} | fun/0 | {fun/x,Args}
%% @return {ok, Result} | {error,timeout}
trans(LockName, Fun) when is_binary(LockName) ->
  trans(?SERVER, LockName, Fun, ?REDIS_LOCK_TIME, ?REDIS_TRANS_TIMEOUT).

trans(Name, LockName, Fun) when is_binary(LockName) ->
  trans(Name, LockName, Fun, ?REDIS_LOCK_TIME, ?REDIS_TRANS_TIMEOUT).

trans(Name, LockName, Fun, LockTime, Timeout) when Timeout =:= infinity ->
  RandBin = erlang:term_to_binary(erlang:make_ref()),
  trans_i(Name, LockName, RandBin, Fun, LockTime, infinity);
trans(Name, LockName, Fun, LockTime, Timeout) when is_integer(Timeout) ->
  RandBin = erlang:term_to_binary(erlang:make_ref()),
  Now = os:perf_counter(millisecond),
  EndTime = Now + Timeout,
  trans_i(Name, LockName, RandBin, Fun, LockTime, EndTime).

trans_i(Name, LockKey, RandBin, Fun, LockTime, EndTime) ->
  Now = os:perf_counter(millisecond),
  Timeout = case EndTime of
              infinity -> false;
              _ -> Now >= EndTime
            end,
  if
    Timeout ->
      {error, timeout};
    true ->
      case redis_proxy:q(Name, ["SET", LockKey, RandBin, "PX", erlang:integer_to_list(LockTime), "NX"], ?REDIS_TIMEOUT) of
        {ok, <<"OK">>} ->
          Ret = case Fun of
                  {M, F, A} -> catch apply(M, F, A);
                  {F, A} when is_function(F) -> catch erlang:apply(F, A);
                  _ when is_function(Fun, 0) -> catch Fun()
                end,
          case redis_proxy:q(Name, ["GET", LockKey], ?REDIS_TIMEOUT) of
            {ok, RandBin} -> redis_proxy:q(Name, ["DEL", LockKey], ?REDIS_TIMEOUT);
            _ -> pass
          end,
          {ok, Ret};
        {ok, undefined} ->
          Left = EndTime - os:perf_counter(millisecond),
          if
            Left =< 1 ->
              {error, timeout};
            true ->
              Random = rand:uniform(?TRANS_SLEEP_TIME_MAX),
              Sleep = erlang:min(Random, Left - 1),
              timer:sleep(Sleep),
              trans_i(Name, LockKey, RandBin, Fun, LockTime, EndTime)
          end;
        Err ->
          % redis error
          Err
      end
  end.
%%%%%%%
join_command(C, K) when is_binary(K) ->
  [C, K];
join_command(C, L) when is_list(L) ->
  join_command(L, C, []);
join_command(C, M) when is_map(M) ->
  join_command(maps:to_list(M), C, []).

join_command([], C, Acc) ->
  [C | lists:reverse(Acc)];
join_command([E | L], C, Acc) when is_binary(E);is_list(E) ->
  join_command(L, C, [E | Acc]);
join_command([{K, V} | L], C, Acc) ->
  join_command(L, C, [V, K | Acc]).

exec(Name, CMD, Timeout, Parse) ->
  case q(Name, CMD, Timeout) of
    {ok, Result} -> {ok, Parse(Result)};
    Err -> Err
  end.

%% keys

%% @doc del(<<"foo">>) or del([<<"foo">>,<<"bar">>...])
del(L) ->
  del(?SERVER, L, ?TIMEOUT).


del(Name, L, Timeout) ->
  exec(Name, join_command(<<"DEL">>, L), Timeout, ?INTEGER_REPLY_FUN).

exists(L) ->
  exists(?SERVER, L, ?TIMEOUT).


exists(Name, L, Timeout) ->
  exec(Name, join_command(<<"EXISTS">>, L), Timeout, ?INTEGER_REPLY_FUN).

expire(K, Sec) ->
  expire(?SERVER, K, Sec, ?TIMEOUT).

expire(Name, K, Sec, Timeout) when is_binary(K), is_integer(Sec) ->
  exec(Name, join_command(<<"EXPIRE">>, [K, erlang:integer_to_binary(Sec)]), Timeout, ?INTEGER_REPLY_FUN).


ttl(K) ->
  ttl(?SERVER, K, ?TIMEOUT).

ttl(Name, K, Timeout) when is_binary(K) ->
  exec(Name, join_command(<<"EXISTS">>, K), Timeout, ?INTEGER_REPLY_FUN).

type(K) ->
  type(?SERVER, K, ?TIMEOUT).

%% @doc string,list,set,zset,hash,stream
type(Name, K, Timeout) when is_binary(K) ->
  Func = fun(X) ->
    if
      X == <<"string">> -> string;
      X == <<"list">> -> list;
      X == <<"set">> -> set;
      X == <<"zset">> -> zset;
      X == <<"hash">> -> hash;
      X == <<"stream">> -> stream;
      true -> erlang:binary_to_atom(X)
    end end,
  exec(Name, join_command(<<"TYPE">>, K), Timeout, Func).

%% string
append(K, V) ->
  append(?SERVER, K, V, ?TIMEOUT).

append(Name, K, V, Timeout) ->
  exec(Name, join_command(<<"APPEND">>, [K, V]), Timeout, ?INTEGER_REPLY_FUN).

decr(K) ->
  decr(?SERVER, K, ?TIMEOUT).

decr(Name, K, Timeout) when is_binary(K) ->
  exec(Name, join_command(<<"DECR">>, K), Timeout, ?INTEGER_REPLY_FUN).

decrby(K, V) ->
  decrby(?SERVER, K, V, ?TIMEOUT).

decrby(Name, K, V, Timeout) when is_binary(K), is_integer(V) ->
  exec(Name, join_command(<<"DECRBY">>, [K, erlang:integer_to_binary(V)]), Timeout, ?INTEGER_REPLY_FUN).

get(K) -> get(?SERVER, K, ?TIMEOUT).

get(Name, K, Timeout) when is_binary(K) ->
  exec(Name, join_command(<<"GET">>, K), Timeout, ?REPLY_FUN).

get_del(K) -> get_del(?SERVER, K, ?TIMEOUT).

get_del(Name, K, Timeout) when is_binary(K) ->
  exec(Name, join_command(<<"GETDEL">>, K), Timeout, ?REPLY_FUN).

get_ex(K, Args) ->
  get_ex(?SERVER, K, Args, ?TIMEOUT).

get_ex(Name, K, Args, Timeout) when is_binary(K), is_list(Args) ->
  exec(Name, join_command(<<"GETEX">>, [K | Args]), Timeout, ?REPLY_FUN).

get_range(K, S, E) ->
  get_range(?SERVER, K, S, E, ?TIMEOUT).

get_range(Name, K, S, E, Timeout) when is_binary(K), is_integer(S), is_integer(E), S =< E ->
  exec(Name, join_command(<<"DECRBY">>, [K, erlang:integer_to_binary(S), erlang:integer_to_binary(E)]), Timeout, ?REPLY_FUN).

incr(K) ->
  incr(?SERVER, K, ?TIMEOUT).
incr(Name, K, Timeout) ->
  exec(Name, join_command(<<"INCR">>, K), Timeout, ?INTEGER_REPLY_FUN).

incrby(K, V) -> incrby(?SERVER, K, V, ?TIMEOUT).

incrby(Name, K, V, Timeout) when is_binary(K), is_integer(V) ->
  exec(Name, join_command(<<"INCRBY">>, [K, erlang:integer_to_binary(V)]), Timeout, ?INTEGER_REPLY_FUN).

mget(L) ->
  mget(?SERVER, L, ?TIMEOUT).

mget(Name, L, Timeout) ->
  exec(Name, join_command(<<"MGET">>, L), Timeout, ?REPLY_FUN).

mset(MapOrList) ->
  mset(?SERVER, MapOrList, ?TIMEOUT).

mset(Name, MapOrList, Timeout) ->
  exec(Name, join_command(<<"MSET">>, MapOrList), Timeout, ?BOOL_REPLY_FUN).

mset_nx(MapOrList) ->
  mset_nx(?SERVER, MapOrList, ?TIMEOUT).

mset_nx(Name, MapOrList, Timeout) ->
  exec(Name, join_command(<<"MSETNX">>, MapOrList), Timeout, ?BOOL_REPLY_FUN).

set(K, V, Args) ->
  set(?SERVER, K, V, Args, ?TIMEOUT).

set(Name, K, V, Args, Timeout) ->
  CMD = [<<"SET">>, K, V | Args],
  exec(Name, CMD, Timeout, ?BOOL_REPLY_FUN).

set_ex(K, V, Expire) ->
  set_ex(?SERVER, K, V, Expire, ?TIMEOUT).

set_ex(Name, K, V, Expire, Timeout) when is_binary(K), is_binary(V), is_integer(Expire) ->
  exec(Name, join_command(<<"SETEX">>, [K, erlang:integer_to_binary(Expire), V]), Timeout, ?BOOL_REPLY_FUN).

set_nx(K, V) ->
  set_nx(?SERVER, K, V, ?TIMEOUT).

set_nx(Name, K, V, Timeout) when is_binary(K), is_binary(V) ->
  exec(Name, join_command(<<"SETEX">>, [K, V]), Timeout, ?INTEGER_REPLY_FUN).

set_range(K, Offset, V) ->
  set_range(?SERVER, K, Offset, V, ?TIMEOUT).

set_range(Name, K, Offset, V, Timeout) when is_binary(K), is_binary(V), is_integer(Offset) ->
  exec(Name, join_command(<<"SETRANGE">>, [K, erlang:integer_to_binary(Offset), V]), Timeout, ?INTEGER_REPLY_FUN).

%% bitmaps







