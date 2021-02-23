-module(ttl_cache).

-behaviour(gen_server).

%% API
-export([
  start_link/0,
  start_link/2
]).

-export([
  put/2,
  put/3,
  put/4,
  get/1,
  get/2,
  get_ttl/1,
  get_ttl/2,
  set_ttl/2,
  set_ttl/3
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).
-define(MAX_TIMEOUT, 1000 * 60).
-define(CALL_TIMEOUT, 5000).
-define(DEFAULT_TTL, 1000 * 60 * 10).

-record(state, {
  name,
  cache_ets, % {Timeout,Ref} -> Key
  timeout_ets, % Key -> {Value,{Timeout,Ref}}
  ttl
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  start_link(?MODULE, []).

start_link(Name, Opt) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name, Opt], []).

put(K, V) ->
  gen_server:call(?MODULE, {put, K, V}, ?CALL_TIMEOUT).

put(K, V, TTL) ->
  gen_server:call(?MODULE, {put, K, V, TTL}, ?CALL_TIMEOUT).

put(Name, K, V, TTL) ->
  gen_server:call(Name, {put, K, V, TTL}, ?CALL_TIMEOUT).

get(K) ->
  ?MODULE:get(?MODULE, K).

get(Name, K) ->
  CacheETS = cache_ets_name(Name),
  case ets:lookup(CacheETS, K) of
    [{K, {V, _}}] -> {ok, V};
    [] -> {error, not_found}
  end.

get_ttl(K) ->
  get_ttl(?MODULE, K).

get_ttl(Name, K) ->
  CacheETS = cache_ets_name(Name),
  case ets:lookup(CacheETS, K) of
    [{K, {_, {TTL, _}}}] -> {ok, TTL};
    [] -> {error, not_found}
  end.

set_ttl(K, TTL) ->
  set_ttl(?MODULE, K, TTL).

set_ttl(Name, K, TTL) ->
  CacheETS = cache_ets_name(Name),
  case ets:lookup(CacheETS, K) of
    [_] ->
      gen_server:call(Name, {set_ttl, K, TTL}, ?CALL_TIMEOUT);
    [] ->
      {error, not_found}
  end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name, Opt]) ->
  CacheETS = cache_ets_name(Name),
  TimeoutETS = timeout_ets_name(Name),
  ets:new(CacheETS, [named_table, protected]),
  ets:new(TimeoutETS, [named_table, ordered_set, protected]),
  TTL = proplists:get_value(ttl, Opt, ?DEFAULT_TTL),
  {ok, #state{name = Name, cache_ets = CacheETS, timeout_ets = TimeoutETS, ttl = TTL}}.

handle_call({put, K, V}, _From, #state{ttl = TTL} = State) ->
  do_put(K, V, TTL, State);
handle_call({put, K, V, TTL}, _From, State) ->
  do_put(K, V, TTL, State);
handle_call({set_ttl, K, TTL}, _From, State) ->
  do_set_ttl(K, TTL, State);
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  Now = timenow_microsecond(),
  Timeout = timeout(Now, State),
  {noreply, State, Timeout};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
cache_ets_name(Name) ->
  erlang:list_to_atom(lists:concat([Name, "_cache_ets"])).

timeout_ets_name(Name) ->
  erlang:list_to_atom(lists:concat([Name, "_timeout_ets"])).

timenow_microsecond() ->
  os:perf_counter(microsecond).

do_put(K, V, TTL, #state{cache_ets = CacheETS, timeout_ets = TimeoutETS} = S) ->
  Now = timenow_microsecond(),
  % delete exist record
  case ets:lookup(CacheETS, K) of
    [{K, {_, TK}}] ->
      ets:delete(TimeoutETS, TK);
    [] ->
      pass
  end,
  TimeoutKey = {Now + 1000 * TTL, erlang:make_ref()},
  ets:insert(TimeoutETS, {TimeoutKey, K}),
  ets:insert(CacheETS, {K, {V, TimeoutKey}}),
  Timeout = timeout(Now, S),
  {reply, ok, S, Timeout}.

do_set_ttl(K, TTL, #state{cache_ets = CacheETS, timeout_ets = TimeoutETS} = S) ->
  Now = timenow_microsecond(),
  case ets:lookup(CacheETS, K) of
    [{_, {V, TK}}] ->
      ets:delete(TimeoutETS, TK),
      TimeoutKey = {Now + 1000 * TTL, erlang:make_ref()},
      ets:insert(TimeoutETS, {TimeoutKey, K}),
      ets:insert(CacheETS, {K, {V, TimeoutKey}});
    [] ->
      pass
  end,
  Timeout = timeout(Now, S),
  {reply, ok, S, Timeout}.

timeout(Now, #state{cache_ets = CacheETS, timeout_ets = TimeoutETS} = State) ->
  case ets:first(TimeoutETS) of
    '$end_of_table' ->
      infinity;
    {Time, _Ref} when Time > Now ->
      erlang:min(Time - Now, ?MAX_TIMEOUT);
    TimeoutKey ->
      case ets:lookup(TimeoutETS, TimeoutKey) of
        [{_, K}] -> ets:delete(CacheETS, K);
        [] -> pass
      end,
      ets:delete(TimeoutETS, TimeoutKey),
      timeout(Now, State)
  end.
