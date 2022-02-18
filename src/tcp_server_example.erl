-module(tcp_server_example).

-export([
  loop/2,
  loop/3
]).

-export([start_server/1]).
-export([start_client/2, get/2, put/3, del/2]).
%% API

start_server(Port) ->
  TcpOpt = [
    binary,
    {packet, 2},
    {backlog, 2048},
    {reuseaddr, true},
    {send_timeout, 5000}
  ],
  tcp_server:start_link(tcp_server, Port, {?MODULE, loop}, undefined, TcpOpt, #{}).

loop(Parent, Socket) ->
  loop(Parent, Socket, #{}).

loop(Parent, S, M) ->
  inet:setopts(S, [{active, once}]),
  receive
    {tcp, S, Data} ->
      Req = erlang:binary_to_term(Data),
      {Resp, M2} = handle_req(Req, M),
      gen_tcp:send(S, erlang:term_to_binary(Resp)),
      loop(Parent, S, M2);
    {tcp_closed, S} ->
      io:format("Socket ~w closed [~w]~n", [S, self()]),
      ok;
    {'EXIT', Parent, Reason} ->
      io:format("socket ~w closed,reason ~p", [S, Reason]),
      ok;
    {tcp_passive, S} ->
      loop(Parent, S, M)
  end.

handle_req({get, K}, M) ->
  case M of
    #{K := V} -> {{ok, V}, M};
    _ -> {{error, not_found}, M}
  end;
handle_req({put, K, V}, M) ->
  M2 = M#{K => V},
  {{ok, ok}, M2};
handle_req({del, K}, M) ->
  case M of
    #{K := V} -> {{ok, V}, maps:remove(K, M)};
    _ -> {{error, not_found}, M}
  end.

%% ====================
start_client(Addr, Port) ->
  Pid = erlang:spawn_link(fun() -> init(Addr, Port) end),
  {ok, Pid}.

init(Addr, Port) ->
  process_flag(trap_exit, true),
  TcpOpt = [binary, {packet, 2}, {send_timeout, 5000}],
  {ok, S} = gen_tcp:connect(Addr, Port, TcpOpt),
  loop_client(S, queue:new()).

loop_client(S, Q) ->
  inet:setopts(S, [{active, once}]),
  receive
    {tcp, S, Data} ->
      Resp = erlang:binary_to_term(Data),
      {{value, FROM}, Q2} = queue:out(Q),
      FROM ! {self(), Resp},
      loop(S, Q2);
    {From, Req} when is_pid(From) ->
      gen_tcp:send(S, erlang:term_to_binary(Req)),
      Q2 = queue:in(From, Q),
      loop(S, Q2);
    {tcp_closed, S} ->
      io:format("Socket ~w closed [~w]~n", [S, self()]),
      ok
  end.

get(Pid, K) ->
  Req = {get, K},
  req(Pid, Req).

put(Pid, K, V) ->
  Req = {put, K, V},
  req(Pid, Req).

del(Pid, K) ->
  Req = {del, K},
  req(Pid, Req).

req(Pid, Req) ->
  S = self(),
  Pid ! {S, Req},
  receive
    {Pid, Resp} -> Resp
  end.