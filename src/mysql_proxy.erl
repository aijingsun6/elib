-module(mysql_proxy).

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
  query/2,
  query/3,
  query/4
]).

-export([
  transaction/2,
  transaction/3,
  transaction/4
]).

-define(SERVER, ?MODULE).
-define(DEFAULT_POOL_SIZE, 8).
-define(TIMEOUT, 5000).

start_link(Args) ->
  L = parse_config(Args),
  poolboy:start_link(L).

start_link(Name, Args) when is_atom(Name) ->
  L = parse_config(Args),
  L2 = [{name, {local, Name}} | L],
  poolboy:start_link(L2).

parse_config(M) when is_map(M) ->
  Config = #{worker_module => mysql_worker, size => ?DEFAULT_POOL_SIZE},
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

query(Query, Param) ->
  query(?SERVER, Query, Param, ?TIMEOUT).

query(Query, Param, Timeout) ->
  query(?SERVER, Query, Param, Timeout).

query(Name, Query, Param, Timeout) ->
  Reply = poolboy:transaction(Name, fun(PID) -> mysql_worker:query(PID, Query, Param, Timeout) end, Timeout),
  case Reply of
    {ok, ColumnNames, Rows} -> {ok, lists:map(fun(X) -> parse_select(ColumnNames, X, #{}) end, Rows)};
    {ok, Modify} -> {ok, Modify};
    Err -> Err
  end.

parse_select([], [], Acc) ->
  Acc;
parse_select([X | L1], [Y | L2], Acc) ->
  parse_select(L1, L2, Acc#{X => Y}).

transaction(Fun, Args) ->
  transaction(?SERVER, Fun, Args, ?TIMEOUT).

transaction(Fun, Args, Timeout) ->
  transaction(?SERVER, Fun, Args, Timeout).

transaction(Name, Fun, Args, Timeout) ->
  Reply = poolboy:transaction(Name, fun(PID) -> mysql_worker:transaction(PID, Fun, Args, Timeout) end, Timeout),
  case Reply of
    {aborted, Reason} ->
      {error, Reason};
    {atomic, {ok, ColumnNames, Rows}} ->
      {ok, lists:map(fun(X) -> parse_select(ColumnNames, X, #{}) end, Rows)};
    {atomic, {ok, Modify}} ->
      {ok, Modify};
    {atomic, Err} ->
      Err
  end.




