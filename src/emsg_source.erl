%%%-------------------------------------------------------------------
%%% @author alking
%%% @copyright
%%% @doc
%%% 针对多语言做的小工具
%%% @end
%%%-------------------------------------------------------------------
-module(emsg_source).
-author('alking').

-behaviour(gen_server).

-define(DEF_LANG, deflang).

-export([
  start_link/3,
  find_msg/3,
  find_msg/4,
  reload/1
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

-record(state, {
  name,
  ets,
  dir,
  basename
}).

start_link(Name, Dir, BaseName) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name, Dir, BaseName], []).

find_msg(SVR, Key, Args) ->
  find_msg(SVR, Key, Args, ?DEF_LANG).

find_msg(SVR, Key, Args, Locale) when is_atom(SVR), Locale == ?DEF_LANG ->
  ETS = ets_name(SVR),
  case ets:lookup(ETS, {Locale, Key}) of
    [{_, Format}] -> {ok, format_msg(Format, Args)};
    [] ->
      {fail, not_found}
  end;
find_msg(SVR, Key, Args, Locale) when is_atom(SVR) ->
  ETS = ets_name(SVR),
  case ets:lookup(ETS, {Locale, Key}) of
    [{_, Format}] -> {ok, format_msg(Format, Args)};
    [] ->
      case ets:lookup(ETS, {?DEF_LANG, Key}) of
        [{_, Format}] -> {ok, format_msg(Format, Args)};
        [] -> {fail, not_found}
      end
  end.

reload(SVR) ->
  gen_server:cast(SVR, {reload}).

format_msg(Format, []) -> Format;
format_msg(Format, Args) ->
  lists:flatten(io_lib:format(Format, Args)).

init([Name, Dir, BaseName]) ->
  ETS = ets_name(Name),
  % { {Lang,Key},Value}
  ets:new(ETS, [public, named_table, set, {read_concurrency, true}]),
  case filelib:is_dir(Dir) of
    false ->
      {stop, dir_error};
    true ->
      load_data(ETS, Dir, BaseName),
      {ok, #state{name = Name, ets = ETS, dir = Dir, basename = BaseName}}
  end.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.
handle_cast({reload}, #state{ets = ETS, dir = Dir, basename = BaseName} = State) ->
  try
    load_data(ETS, Dir, BaseName)
  catch
    _: _ -> ok
  end,
  {noreply, State};
handle_cast(_Request, State) ->
  {noreply, State}.
handle_info(_Info, State) ->
  {noreply, State}.
terminate(_Reason, _State) ->
  ok.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

ets_name(Name) when is_atom(Name) ->
  erlang:list_to_atom(lists:concat([Name, "_msg_source_ets"])).

load_data(ETS, Dir, BaseName) ->
  RE = lists:flatten(io_lib:format("~s*.properties", [BaseName])),
  L = filelib:wildcard(RE, Dir),
  load_data(L, ETS, Dir, BaseName).

load_data([], _ETS, _Dir, _BaseName) -> ok;
load_data([FileName | L], ETS, Dir, BaseName) ->
  Lang = parse_lang(FileName, BaseName),
  FileName2 = filename:join(Dir, FileName),
  Acc = eproperties:read(FileName2),
  [ets:insert(ETS, {{Lang, K}, delete_return(V)}) || {K, V} <- Acc],
  load_data(L, ETS, Dir, BaseName).

delete_return(Line) ->
  case lists:reverse(Line) of
    [$\n | Line2] -> lists:reverse(Line2);
    [$\r | Line2] -> lists:reverse(Line2);
    _ -> Line
  end.

parse_lang(FileName, BaseName) ->
  L = lists:flatten(io_lib:format("~s.properties", [BaseName])),
  case L == FileName of
    true ->
      ?DEF_LANG;
    false ->
      Regex = lists:flatten(io_lib:format("~s_(.*)\\.properties", [BaseName])),
      {ok, MP} = re:compile(Regex),
      {match, [_, {S, E}]} = re:run(FileName, MP),
      lists:sublist(FileName, S + 1, E)
  end.