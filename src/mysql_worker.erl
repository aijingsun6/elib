-module(mysql_worker).
-include_lib("kernel/include/logger.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([query/4, transaction/4]).

-record(state, {
  name,
  conn
}).

-define(TIMEOUT, 5000).

start_link(Args) ->
  gen_server:start_link(?MODULE, [Args], []).

query(P, Query, Params, Timeout) when is_binary(Query) ->
  gen_server:call(P, {query, Query, Params, Timeout}, Timeout).

transaction(P, Fun, Args, Timeout) ->
  gen_server:call(P, {transaction, Fun, Args}, Timeout).

init([Args]) ->
  process_flag(trap_exit, true),
  Name = case lists:keyfind(name, 1, Args) of
           {name, {local, Name0}} -> Name0;
           _ -> ?MODULE
         end,
  ?LOG_INFO("~ts start...", [Name]),
  Args2 = lists:keydelete(name, 1, Args),
  {ok, Conn} = mysql:start_link(Args2),
  {ok, #state{name = Name, conn = Conn}}.

handle_call({query, Query, Params, Timeout}, _From, #state{conn = Con} = State) ->
  Reply = handle_query(Con, Query, Params, Timeout),
  {reply, Reply, State};
handle_call({transaction, Fun, Args}, _From, #state{conn = Con} = State) ->
  Reply = handle_transaction(Con, Fun, Args),
  {reply, Reply, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{name = Name, conn = Conn}) ->
  ?LOG_INFO("~ts stop...", [Name]),
  ok = mysql:stop(Conn),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

sql_type(SQL) when is_binary(SQL) ->
  L = string:to_upper(string:trim(erlang:binary_to_list(SQL))),
  % insert,select,update,delete
  case L of
    [$I, $N, $S, $E, $R, $T | _] -> {ok, insert};
    [$S, $E, $L, $E, $C, $T | _] -> {ok, select};
    [$U, $P, $D, $A, $T, $E | _] -> {ok, update};
    [$D, $E, $L, $E, $T, $E | _] -> {ok, delete};
    [$R, $E, $P, $L, $A, $C, $E | _] -> {ok, replace};
    _ -> {error, unknown}
  end.

handle_query(C, SQL, Params, Timeout) ->
  case mysql:query(C, SQL, Params, Timeout) of
    {ok, ColumnNames, Rows} ->
      {ok, ColumnNames, Rows};
    ok ->
      M = #{affected_rows => mysql:affected_rows(C), warning_count => mysql:warning_count(C)},
      case sql_type(SQL) of
        {ok, Type} when Type =:= insert orelse Type =:= replace ->
          LastInsertId = mysql:insert_id(C),
          {ok, M#{insert_id => LastInsertId}};
        _ ->
          {ok, M}
      end;
    {error, Err} ->
      {error, format_error(Err)}
  end.

handle_transaction(C, Fun, Args) ->
  mysql:transaction(C, fun() -> erlang:apply(Fun, [C | Args]) end).

format_error({Code, State, Msg}) ->
  #{code => Code, sql_state => State, msg => Msg}.