-module(ttl_monitor_cache).
%% 带monitor的ttl服务，缓存svr,并且monitor如果发现服务down掉就移除
%% 主要是监控分布式的svr
%% svr 格式：
%% pid | {Name,Node}
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  start_link/1
]).

-export([
  find_svr/1,
  find_svr/2,
  put_svr/2,
  put_svr/3,
  put_svr/4
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
-define(CALL_TIMEOUT, 5000).
-define(MAX_TIMEOUT, 1000 * 60).
-define(DEF_TTL, 1000 * 60 * 5).

-record(state, {
  cache_ets, %  { Key, Svr, Ref,  Timeout }
  ref_ets,   %  { Ref, Key, Timeout }
  timeout_ets % { Timeout, Key, Ref }
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  start_link(?MODULE).

start_link(Name) when is_atom(Name) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name], []).

find_svr(Key) ->
  find_svr(?MODULE, Key).

find_svr(Name, Key) ->
  ETS = cache_ets_name(Name),
  case ets:lookup(ETS, Key) of
    [{_, Svr, _, _}] -> {ok, Svr};
    [] -> {error, not_found}
  end.

put_svr(Key, Svr) ->
  put_svr(?MODULE, Key, Svr, ?DEF_TTL).

put_svr(Key, Svr, TTL) ->
  put_svr(?MODULE, Key, Svr, TTL).

put_svr(Name, Key, Svr, TTL) ->
  gen_server:call(Name, {put, Key, Svr, TTL}, ?CALL_TIMEOUT).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name]) ->
  CacheETS = cache_ets_name(Name),
  Ref = ref_ets_name(Name),
  TimeoutETS = timeout_ets_name(Name),
  ets:new(CacheETS, [named_table, protected]),
  ets:new(Ref, [named_table, protected]),
  ets:new(TimeoutETS, [named_table, ordered_set, protected]),
  {ok, #state{cache_ets = CacheETS, ref_ets = Ref, timeout_ets = TimeoutETS}}.

handle_call({put, Key, Svr, TTL}, _From, State) ->
  handle_put(Key, Svr, TTL, State);
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  Now = timenow_microsecond(),
  case timeout(Now, State) of
    Timeout when is_integer(Timeout) -> {noreply, State, Timeout};
    _ -> {noreply, State}
  end;
handle_info({'DOWN', MonitorRef, _Type, _Object, _Info}, State) ->
  handle_down(MonitorRef, State);
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

ref_ets_name(Name) ->
  erlang:list_to_atom(lists:concat([Name, "_ref_ets"])).

timeout_ets_name(Name) ->
  erlang:list_to_atom(lists:concat([Name, "_timeout_ets"])).

handle_put(Key, Svr, TTL, #state{cache_ets = CacheETS, ref_ets = RefETS, timeout_ets = TimeoutETS} = State) ->
  Now = timenow_microsecond(),
  case ets:lookup(CacheETS, Key) of
    [{Key, Svr, _Ref, _Timeout}] ->
      {reply, pass, State, timeout(Now, State)};
    [{Key, _OldSvr, OldRef, OldTimeout}] ->
      case monitor_svr(Svr) of
        Ref when is_reference(Ref) ->
          erlang:demonitor(OldRef),
          ets:delete(RefETS, OldRef),
          ets:delete(TimeoutETS, OldTimeout),
          Timeout = Now + TTL * 1000,
          ets:insert(CacheETS, {Key, Svr, Ref, Timeout}),
          ets:insert(RefETS, {Ref, Key, Timeout}),
          ets:insert(TimeoutETS, {Timeout, Key, Ref}),
          {reply, ok, State, timeout(Now, State)};
        Err ->
          {reply, Err, State, timeout(Now, State)}
      end;
    [] ->
      case monitor_svr(Svr) of
        Ref when is_reference(Ref) ->
          Timeout = Now + TTL,
          ets:insert(CacheETS, {Key, Svr, Ref, Timeout}),
          ets:insert(RefETS, {Ref, Key, Timeout}),
          ets:insert(TimeoutETS, {Timeout, Key, Ref}),
          {reply, ok, State, timeout(Now, State)};
        Err ->
          {reply, Err, State, timeout(Now, State)}
      end
  end.

monitor_svr(Pid) when is_pid(Pid) ->
  erlang:monitor(process, Pid);
monitor_svr({Svr, Node}) when is_atom(Node), is_atom(Svr) ->
  erlang:monitor(process, {Svr, Node});
monitor_svr(_) ->
  svr_format_error.

timenow_microsecond() ->
  os:perf_counter(microsecond).

timeout(Now, #state{cache_ets = CacheETS, ref_ets = RefETS, timeout_ets = TimeoutETS} = State) ->
  case ets:first(TimeoutETS) of
    '$end_of_table' ->
      infinity;
    {Timeout, _Ref} when Timeout > Now ->
      erlang:min(Timeout - Now, ?MAX_TIMEOUT);
    TimeoutKey ->
      case ets:lookup(TimeoutETS, TimeoutKey) of
        [{_, Key, Ref}] ->
          ets:delete(CacheETS, Key),
          erlang:demonitor(Ref),
          ets:delete(RefETS, Ref);
        [] ->
          pass
      end,
      ets:delete(TimeoutETS, TimeoutKey),
      timeout(Now, State)
  end.

handle_down(Ref, #state{cache_ets = CacheETS, ref_ets = RefETS, timeout_ets = TimeoutETS} = State) ->
  Now = timenow_microsecond(),
  case ets:lookup(RefETS, Ref) of
    [{Ref, Key, Timeout}] ->
      erlang:demonitor(Ref),
      ets:delete(CacheETS, Key),
      ets:delete(RefETS,Ref),
      ets:delete(TimeoutETS, Timeout),
      {noreply, State, timeout(Now, State)};
    [] ->
      {noreply, State, timeout(Now, State)}
  end.