%%%-------------------------------------------------------------------
%%% @author alking
%%% @copyright
%%% @doc
%%% 读取，编辑properties 文件
%%% 执行标准：https://en.wikipedia.org/wiki/.properties
%%% TODO:添加转义字符处理，添加形如 \uxxxx的unicode字符处理
%%% @end
%%%-------------------------------------------------------------------
-module(eproperties).
-include_lib("kernel/include/logger.hrl").
-define(ESCAPE_CHAR, $\\).
%% API
-export([
  read/1,
  write/2
]).

read(FilePath) ->
  case file:open(FilePath, [read, binary]) of
    {ok, IO} ->
      read_i(IO, [], []);
    Err ->
      Err
  end.

read_i(IO, Acc, LastLine) ->
  case file:read_line(IO) of
    {ok, Bin} ->
      Line = lists:flatten(io_lib:format("~ts", [Bin])),
      {Acc2, LastRec2} = read_acc(Line, Acc, LastLine),
      read_i(IO, Acc2, LastRec2);
    eof ->
      Acc
  end.

read_acc([$# | _], Acc, LastLine) -> {Acc, LastLine};
read_acc([$! | _], Acc, LastLine) -> {Acc, LastLine};
read_acc([], Acc, LastLine) -> {Acc, LastLine};
read_acc(Line, Acc, LastLine) ->
  [Last | L] = lists:reverse(Line),
  case Last == ?ESCAPE_CHAR of
    true ->
      {Acc, LastLine ++ lists:reverse(L)};
    false ->
      Line2 = LastLine ++ Line,
      case parse_line(Line2, []) of
        {error, _} ->
          ?LOG_ERROR("parse line ~ts error", [Line2]),
          {Acc, []};
        {K, V} ->
          {[{K, V} | Acc], []}
      end
  end.

parse_line([$= | Left], Acc) -> {lists:reverse(Acc), Left};
parse_line([$: | Left], Acc) -> {lists:reverse(Acc), Left};
parse_line([], _) ->
  {error, format_error};
parse_line([X | L], Acc) ->
  parse_line(L, [X | Acc]).

write(File, PL) ->
  Con = build_content(PL, []),
  file:write_file(File, Con).

build_content([], Acc) -> lists:reverse(Acc);
build_content([{K, V} | L], Acc) ->
  Bin = unicode:characters_to_binary(lists:flatten(io_lib:format("~ts=~ts~n", [K, V]))),
  build_content(L, [Bin | Acc]).