-module(tcp_server).
-include_lib("kernel/include/logger.hrl").
-behavior(gen_server).

-define(ACCEPT_NUM_DEFAULT, 8).
-define(CONNECT_MAX_DEFAULT, 1024000).

-define(TCP_OPTIONS_DEFAULT, [
  binary,             % 传输的是二进制
  {packet, raw},      % 不封包
  {active, true},
  {backlog, 2048},
  {reuseaddr, true},
  {send_timeout, 5000}
]).
-ifdef(OTP_RELEASE). %% this implies 21 or higher
-define(EXCEPTION(Class, Reason, Stacktrace), Class:Reason:Stacktrace).
-define(GET_STACK(Stacktrace), Stacktrace).
-else.
-define(EXCEPTION(Class, Reason, _), Class:Reason).
-define(GET_STACK(_), erlang:get_stacktrace()).
-endif.

-record(state, {
  name,
  pid,
  port,
  accept_num,
  tcp_options,
  listen_sock,             % listen socket
  conn_cur,                % current connection
  conn_max,                % max connection
  loop,                    % {M,F}  M:F(Pid,Socket)
  reject,                  % {M,F}  M:F(Pid,Socket)
  tab                  % {Pid, Socket}
}).

-callback loop(Father :: pid(), S :: inet:socket()) -> any().
-callback reject(Father :: pid(), S :: inet:socket()) -> any().

-export([
  init/1,
  code_change/3,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

-export([
  start_link/4,
  start_link/6
]).

start_link(Name, Port, Loop, Reject) ->
  start_link(Name, Port, Loop, Reject, ?TCP_OPTIONS_DEFAULT, #{}).

start_link(Name, Port, Loop, Reject, TcpOption, Prop) ->
  ConnMax = maps:get(conn_max, Prop, ?CONNECT_MAX_DEFAULT),
  AcceptNum = maps:get(accept_num, Prop, ?ACCEPT_NUM_DEFAULT),
  true = (is_integer(ConnMax) andalso ConnMax > 0),
  true = (is_integer(AcceptNum) andalso AcceptNum > 0),
  S = #state{
    name = Name,
    port = Port,
    conn_cur = 0,
    conn_max = ConnMax,
    accept_num = AcceptNum,
    tcp_options = TcpOption,
    loop = Loop,
    reject = Reject
  },
  gen_server:start_link({local, Name}, ?MODULE, S, []).

tab_name(Name) ->
  erlang:list_to_atom(lists:concat([?MODULE, "_", Name])).

init(S = #state{name = Name, port = Port, accept_num = AcceptNum, tcp_options = TcpOptions}) ->
  process_flag(trap_exit, true),
  ?LOG_INFO("~p initializing port ~p", [Name, Port]),
  case gen_tcp:listen(Port, TcpOptions) of
    {ok, ListenSock} ->
      ?LOG_INFO("~p,socket listen success at port ~p", [Name, Port]),
      Tab = tab_name(Name),
      ets:new(Tab, [named_table, set]),
      S2 = S#state{listen_sock = ListenSock, tab = Tab, pid = self()},
      start_accept(AcceptNum, S2),
      {ok, S2};
    {error, Why} ->
      ?LOG_ERROR("~p,socket listen fail at port ~p with ~p", [Name, Port, Why]),
      {stop, Why}
  end.

handle_call({check_conn_num, Pid, Socket}, _, #state{conn_cur = Conn, conn_max = MaxConn, tab = Tab} = S) ->
  case Conn < MaxConn of
    true ->
      erlang:link(Pid),
      ets:insert(Tab, {Pid, Socket}),
      {reply, true, S#state{conn_cur = Conn + 1}};
    false ->
      {reply, false, S}
  end;
handle_call(_Msg, _Caller, State) ->
  {noreply, State}.

handle_cast(accept_new, State) ->
  proc_lib:spawn(fun() -> accept(State) end),
  {noreply, State};
handle_cast(_Request, #state{name = Name} = State) ->
  ?LOG_WARNING("~p unhandled cast msg :~p", [Name, _Request]),
  {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #state{name = Name, conn_cur = Conn, tab = Tab} = S) ->
  case ets:lookup(Tab, Pid) of
    [{Pid, Socket}] ->
      ?LOG_INFO("~p, pid ~p, socket ~p disconnect, reason ~p", [Name, Pid, socket_ip_str(Socket), Reason]),
      ets:delete(Tab, Pid),
      {noreply, S#state{conn_cur = Conn - 1}};
    [] ->
      {noreply, S}
  end;
handle_info(_Msg, #state{name = Name} = State) ->
  ?LOG_WARNING("~p unhandled info msg :~p", [Name, _Msg]),
  {noreply, State}.

code_change(_OldVersion, Library, _Extra) ->
  {ok, Library}.

terminate(_Reason, #state{name = Name, listen_sock = ListenSock}) ->
  ?LOG_INFO("~p stopping with ~p", [Name, _Reason]),
  gen_tcp:close(ListenSock),
  ok.

loop_func(Name, Socket, {M, F}) ->
  try
    M:F(Name, Socket)
  catch
    ?EXCEPTION(Class, Reason, Stacktrace) ->
      ?LOG_ERROR("tcp loop fail, stacktrace: ~p, class: ~p, reason: ~p ~n", [?GET_STACK(Stacktrace), Class, Reason])
  end.

reject_func(Name, Socket, {M, F}) ->
  M:F(Name, Socket);
reject_func(_, _, _) ->
  ok.

start_accept(Num, _State) when Num < 1 ->
  ok;
start_accept(Num, State) ->
  proc_lib:spawn(fun() -> accept(State) end),
  start_accept(Num - 1, State).

accept(#state{name = Name, pid = PID, listen_sock = ListenSock, loop = Loop, reject = Reject}) ->
  process_flag(trap_exit, true),
  case gen_tcp:accept(ListenSock) of
    {ok, Socket} ->
      Self = self(),
      ?LOG_INFO("~p,pid:~p,accept socket ~ts", [Name, Self, socket_ip_str(Socket)]),
      gen_server:cast(Name, accept_new),
      case gen_server:call(Name, {check_conn_num, Self, Socket}) of
        true ->
          loop_func(PID, Socket, Loop);
        false ->
          reject_func(PID, Socket, Reject)
      end;
    {error, Reason} ->
      ?LOG_ERROR("~p accept socket fail with ~p", [Name, Reason]),
      gen_server:cast(Name, accept_new),
      ok
  end.

socket_ip_str(Socket) ->
  case inet:peername(Socket) of
    {ok, {IP, Port}} ->
      case inet:ntoa(IP) of
        {error, einval} -> undefined;
        Addr -> lists:flatten(io_lib:format("(~ts:~p)", [Addr, Port]))
      end;
    {error, _} -> undefined
  end.