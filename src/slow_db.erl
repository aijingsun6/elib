%%% @doc
%%% 将数据库写入分摊，减少瞬时IO,将IO平摊
%%% @end
-module(slow_db).
-include_lib("kernel/include/logger.hrl").
-behaviour(gen_server).

%% API
-export([
  start_link/1,
  start_link/2,
  start_link/3,
  put/1,
  put/2,
  remove/1,
  remove/2
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


-type flush_config() :: #{
flush_interval => integer(),% 刷数据库时间间隔
flush_once => integer(), % 每次刷数据库条数
flush_error_drop => boolean() % 单条保存失败后是否取消保存
}.

-record(state, {
  name,
  queue = queue:new(),
  mark_map = #{},
  callback,
  flush_config :: flush_config(),
  flushing = false
}).

start_link(CB) ->
  start_link(?MODULE, CB, #{}).

start_link(CB, FlushCfg) ->
  start_link(?MODULE, CB, FlushCfg).

start_link(Name, CB, FlushCfg) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name, CB, FlushCfg], []).

put(K) ->
  ?MODULE:put(?MODULE, K).

put(Name, K) ->
  gen_server:call(Name, {put, K}, 5000).

remove(K) ->
  remove(?MODULE, K).

remove(Name, K) ->
  gen_server:call(Name, {remove, K}, 5000).

init([Name, CB, FlushCfg0]) ->
  FlushCfg = maps:merge(default_flush_cfg(), FlushCfg0),
  {ok, #state{name = Name, queue = queue:new(), mark_map = #{}, callback = CB, flush_config = FlushCfg}}.

handle_call({put, K}, _From, #state{mark_map = M, queue = Q, flushing = Flushing, flush_config = #{flush_interval:= FI}} = S) ->
  {Q2, M2} = case maps:is_key(K, M) of
               true -> {Q, M};
               false -> {queue:in_r(K, Q), maps:put(K, 1, M)}
             end,
  S2 = S#state{mark_map = M2, queue = Q2},
  case Flushing of
    true -> pass;
    false -> erlang:send_after(FI, self(), flush)
  end,
  {reply, ok, S2#state{flushing = true}};
handle_call({remove, K}, _From, #state{mark_map = M} = State) ->
  M2 = maps:remove(K, M),
  {reply, ok, State#state{mark_map = M2}};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(flush, #state{queue = Q} = S) ->
  case queue:is_empty(Q) of
    true ->
      {noreply, S#state{flushing = false}};
    false ->
      Start = unix_time(),
      S2 = flush_i(S),
      End = unix_time(),
      #state{queue = Q2, flush_config = #{flush_interval:= FI}} = S2,
      case queue:is_empty(Q2) of
        true ->
          {noreply, S2#state{flushing = false}};
        false ->
          Cost = End - Start,
          case Cost > FI of
            true ->
              % flush once
              erlang:send(self(), flush);
            false ->
              erlang:send_after(FI - Cost, self(), flush)
          end,
          {noreply, S2#state{flushing = true}}
      end
  end;
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

default_flush_cfg() ->
  % 1 sec flush 100 records
  #{flush_interval => 1000, flush_once => 100, flush_error_drop => false}.

unix_time() ->
  os:perf_counter(millisecond).

flush_i(#state{name = Name, queue = Q, flush_config = #{flush_once:= FO, flush_error_drop:= Drop}} = S) ->
  {Q2, Q3} = queue:split(FO, Q),
  L = queue:to_list(Q2),
  {Total, Fail, FailRec, S2} = flush_ii(L, S, 0, 0, []),
  ?LOG_DEBUG("~p flush total num:~p,fail num:~p", [Name, Total, Fail]),
  Q4 = case Drop of
         true -> Q3;
         false -> lists:foldl(fun(K, Acc) -> queue:in(K, Acc) end, Q3, FailRec)
       end,
  S2#state{queue = Q4}.

flush_ii([], State, Fail, Total, Acc) ->
  {Total, Fail, Acc, State};
flush_ii([K | L], #state{name = Name, mark_map = Map, callback = CB, flush_config = #{flush_error_drop:= Drop}} = S, Fail, Total, Acc) ->
  case maps:is_key(K, Map) of
    false ->
      flush_ii(L, S, Fail, Total, Acc);
    true ->
      CBReply = case CB of
                  {M, F} -> M:F(K);
                  _ when is_function(CB, 1) -> CB(K)
                end,
      case CBReply of
        ok ->
          Map2 = maps:remove(K, Map),
          flush_ii(L, S#state{mark_map = Map2}, Fail, Total + 1, Acc);
        {error, Reason} ->
          ?LOG_WARNING("~p flush ~p failed with reason ~p", [Name, K, Reason]),
          Map2 = case Drop of
                   true -> maps:remove(K, Map);
                   false -> Map
                 end,
          flush_ii(L, S#state{mark_map = Map2}, Fail + 1, Total + 1, Acc)
      end
  end.