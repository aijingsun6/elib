-module(elib_time).

%% API
-export([
  unix_timestamp/0,
  unix_timestamp_ms/0,
  localtime_to_timestamp/1,
  unix_timestamp_to_utc_time/1,
  unix_timestamp_to_local_time/1,
  time_to_string/1,
  binary_to_time/1,
  string_to_time/1,
  date_to_string/1,
  binary_to_date/1,
  string_to_date/1
]).

unix_timestamp() ->
  os:perf_counter(second).

unix_timestamp_ms() ->
  os:perf_counter(millisecond).

localtime_to_timestamp(LT) ->
  % 62167219200 = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})
  calendar:datetime_to_gregorian_seconds(erlang:localtime_to_universaltime(LT)) - 62167219200.

unix_timestamp_to_utc_time(TimeStamp) ->
  MegaS = TimeStamp div (1000 * 1000),
  S = TimeStamp rem (1000 * 1000),
  calendar:now_to_datetime({MegaS, S, 0}).

unix_timestamp_to_local_time(TimeStamp) ->
  MegaS = TimeStamp div (1000 * 1000),
  S = TimeStamp rem (1000 * 1000),
  calendar:now_to_local_time({MegaS, S, 0}).

time_to_string({{Y, M, D}, {H, MM, S}}) ->
  lists:flatten(io_lib:format("~B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B", [Y, M, D, H, MM, S]));
time_to_string({Y, M, D}) ->
  time_to_string({{Y, M, D}, {0, 0, 0}}).

date_to_string({Y, M, D}) ->
  lists:flatten(io_lib:format("~B-~2.10.0B-~2.10.0B", [Y, M, D])).

binary_to_time(Bin) when is_binary(Bin) ->
  Str = erlang:binary_to_list(Bin),
  string_to_time(Str).

string_to_time(Str) when is_list(Str) ->
  [Y, M, D, H, MM, S] = string:tokens(Str, "- :"),
  F = fun(X) -> {I, _} = string:to_integer(X), I end,
  {{F(Y), F(M), F(D)}, {F(H), F(MM), F(S)}}.

binary_to_date(Bin) when is_binary(Bin) ->
  Str = erlang:binary_to_list(Bin),
  string_to_date(Str).

string_to_date(Str) when is_list(Str) ->
  [Y, M, D] = string:tokens(Str, "-"),
  F = fun(X) -> {I, _} = string:to_integer(X), I end,
  {F(Y), F(M), F(D)}.
