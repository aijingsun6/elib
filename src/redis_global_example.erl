-module(redis_global_example).
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  start_link/1
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


-record(state, {name}).

start_link() ->
  start_link(?MODULE).

start_link(Name) ->
  SName = redis_global:svr_name(Name),
  gen_server:start_link(SName, ?MODULE, [Name], []).

init([Name]) ->
  io:format("~p init...~n", [Name]),
  process_flag(trap_exit, true),
  {ok, #state{name = Name}}.

handle_call(_Request, _From, #state{name = Name} = State) ->
  io:format("~p recv ~p~n", [Name, _Request]),
  {reply, ok, State}.

handle_cast(_Request, #state{name = Name} = State) ->
  io:format("~p recv ~p~n", [Name, _Request]),
  {noreply, State}.

handle_info(_Request, #state{name = Name} = State) ->
  io:format("~p recv ~p~n", [Name, _Request]),
  {noreply, State}.

terminate(Reason, #state{name = Name}) ->
  io:format("~p terminate with ~p~n", [Name, Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.