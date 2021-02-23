%%% @doc queue_task: run task in a queue
%%%
%%% sync: block until task finish, return {ok, ID, Result} | {error,ID,Info}
%%%
%%% async: get result like {error, duplicate_task} | {ok, progressing} | {ok,waiting} immediately,
%%% then receive a message like {ok, ID, Result} | {error,ID,Info}
-module(queue_task).

-include_lib("kernel/include/logger.hrl").
-behaviour(gen_server).

-export([
  start_link/0,
  start_link/2
]).
-export([
  sync/2,
  sync/3,
  async/2,
  async/3
]).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).
-define(SPAWN_OPTS, [{spawn_opt, [{message_queue_data, off_heap}]}]).
-define(TYPE_SYNC, sync).
-define(TYPE_ASYNC, async).
-define(KEY_PROCESS_MAX, process_max).
-define(PROCESS_MAX_DEFAULT, 32).
-record(state, {
  name,
  task_cache_ets,   % {ID, Task, Type, From}
  task_monitor_ets, % {Ref, ID, Pid}
  cfg,
  queue
}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  start_link(?SERVER, #{}).

start_link(Name, ProcessMax) when is_integer(ProcessMax) ->
  start_link(Name, #{?KEY_PROCESS_MAX => ProcessMax});
start_link(Name, Cfg) when is_atom(Name), is_map(Cfg) ->
  Def = default_config(),
  gen_server:start_link({local, Name}, ?MODULE, [Name, maps:merge(Def, Cfg)], ?SPAWN_OPTS).

sync(ID, Task) -> sync(?SERVER, ID, Task).
sync(Name, ID, Task) -> gen_server:call(Name, {sync, ID, Task}, infinity).

async(ID, Task) -> async(?SERVER, ID, Task).
async(Name, ID, Task) -> gen_server:call(Name, {async, ID, Task}, infinity).

init([Name, Cfg]) ->
  process_flag(trap_exit, true),
  TaskCacheETS = task_cache_ets(Name),
  TaskMonitorETS = task_monitor_ets(Name),
  ets:new(TaskCacheETS, [named_table, set, protected, {read_concurrency, true}]),
  ets:new(TaskMonitorETS, [named_table, set, protected, {read_concurrency, true}]),
  {ok, #state{name = Name, task_cache_ets = TaskCacheETS, task_monitor_ets = TaskMonitorETS, cfg = Cfg, queue = queue:new()}}.

handle_call({Type, ID, Task}, From, State = #state{name = Name, task_cache_ets = TaskCacheETS, task_monitor_ets = TaskMonETS, cfg = #{?KEY_PROCESS_MAX := PM}, queue = Q}) ->
  ?LOG_INFO("~p,~p,~p", [Name, Type, ID]),
  case ets:lookup(TaskCacheETS, ID) of
    [{_, _Task, _Type, _From}] ->
      {reply, {error, duplicate_task}, State};
    [] ->
      ets:insert(TaskCacheETS, {ID, Task, Type, From}),
      case ets:info(TaskMonETS, size) < PM of
        true ->
          {Pid, Ref} = do_task(Name, ID, Task),
          ets:insert(TaskMonETS, {Ref, ID, Pid}),
          case Type of
            ?TYPE_SYNC -> {noreply, State};
            ?TYPE_ASYNC -> {reply, {ok, progressing}, State}
          end;
        false ->
          Q2 = queue:in(ID, Q),
          S2 = State#state{queue = Q2},
          case Type of
            ?TYPE_SYNC -> {noreply, S2};
            ?TYPE_ASYNC -> {reply, {ok, waiting}, S2}
          end
      end
  end;
handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast({task_finish, ID, Result}, #state{name = Name, task_cache_ets = TaskCacheETS} = S) ->
  ?LOG_INFO("~p task ~p finish", [Name, ID]),
  reply(TaskCacheETS, ID, {ok, ID, Result}),
  S2 = do_next_task(S),
  {noreply, S2};
handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({'DOWN', _Ref, _Type, _Pid, _Info} = Msg, #state{} = S) ->
  proc_down(Msg, S),
  S2 = do_next_task(S),
  {noreply, S2};
handle_info(Msg, State = #state{name = Name}) ->
  ?LOG_WARNING("~p unhandle msg:~p", [Name, Msg]),
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

default_config() ->
  #{?KEY_PROCESS_MAX => ?PROCESS_MAX_DEFAULT}.

task_cache_ets(Name) ->
  erlang:list_to_atom(lists:concat(["task_cache_", Name])).

task_monitor_ets(Name) ->
  erlang:list_to_atom(lists:concat(["task_monitor_", Name])).

do_task(Name, ID, Task) ->
  P = proc_lib:spawn(fun() -> Ret = do_task_i(Task), gen_server:cast(Name, {task_finish, ID, Ret}) end),
  Ref = erlang:monitor(process, P),
  ?LOG_INFO("~p,do_task ~p, pid:~p", [Name, ID, P]),
  {P, Ref}.

do_task_i(Task) when is_function(Task, 0) -> erlang:apply(Task, []);
do_task_i({M, F, A}) -> erlang:apply(M, F, A).

do_next_task(#state{name = Name, task_cache_ets = TaskCacheETS, task_monitor_ets = TaskMonETS, cfg = #{?KEY_PROCESS_MAX := PM}, queue = Q} = S) ->
  case ets:info(TaskMonETS, size) < PM of
    true ->
      case next_task(Q, TaskCacheETS) of
        {error, Q2} ->
          S#state{queue = Q2};
        {ok, ID, Task, Q2} ->
          {Pid, Ref} = do_task(Name, ID, Task),
          ets:insert(TaskMonETS, {Ref, ID, Pid}),
          S#state{queue = Q2}
      end;
    false ->
      S
  end.

next_task(Q, TaskCacheETS) ->
  case queue:out(Q) of
    {empty, Q2} ->
      {error, Q2};
    {{value, ID}, Q2} ->
      case ets:lookup(TaskCacheETS, ID) of
        [{_, Task, _Type, _From}] ->
          {ok, ID, Task, Q2};
        [] ->
          next_task(Q2, TaskCacheETS)
      end
  end.

reply(TaskCacheETS, ID, Msg) ->
  case ets:lookup(TaskCacheETS, ID) of
    [{_, _Task, Type, {To, _Tag} = From}] ->
      case Type of
        ?TYPE_SYNC -> gen_server:reply(From, Msg);
        ?TYPE_ASYNC -> catch To ! Msg
      end,
      ets:delete(TaskCacheETS, ID);
    [] ->
      pass
  end.

proc_down({'DOWN', Ref, _Type, Pid, Info}, #state{name = Name, task_cache_ets = TaskCacheETS, task_monitor_ets = TaskMonETS}) ->
  erlang:demonitor(Ref, [flush]),
  case ets:lookup(TaskMonETS, Ref) of
    [{_, ID, _}] ->
      ?LOG_INFO("~p task ~p, proc ~p down with ~p", [Name, ID, Pid, Info]),
      ets:delete(TaskMonETS, Ref),
      reply(TaskCacheETS, ID, {error, ID, Info});
    [] ->
      ?LOG_INFO("~p proc ~p down with ~p", [Name, Pid, Info]),
      pass
  end.