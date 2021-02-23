-module(tcp_server).
-include_lib("kernel/include/logger.hrl").
-behavior(gen_server).

-define(ACCEPT_NUM_DEFAULT, 128).
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
  port,           % listen port
  accept_num,
  tcp_options,
  listen_sock,         % listen socket
  conn = 0,       % current connection
  conn_max = 50000,% max connection
  callback,
  sock_map = #{} % ref -> {pid, socket}
}).

-record(callback, {
  loop, % 主循环体，进行服务的函数
  reg,  % 注册函数，在gen_server启动的时候执行，{Module, Func} or fun/0
  close,% 服务器退出回调,{Module, Func} or fun/0
  reject % 拒绝函数，超过最大连接数执行 {Module, Func} or fun/1 (M:F(Name,Socket))
}).
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
  start_link/5,
  start_link/6
]).

start_link(Name, Port, Max, Callback) ->
  start_link(Name, Port, Max, ?ACCEPT_NUM_DEFAULT, Callback, ?TCP_OPTIONS_DEFAULT).

start_link(Name, Port, Max, AcceptNum, Callback) ->
  start_link(Name, Port, Max, AcceptNum, Callback, ?TCP_OPTIONS_DEFAULT).

start_link(Name, Port, Max, AcceptNum, Callback, TcpOptions) ->
  State = #state{
    name = Name,
    port = Port,
    conn = 0,
    conn_max = Max,
    accept_num = AcceptNum,
    tcp_options = TcpOptions,
    callback = func2callback(Callback),
    sock_map = #{}
  },
  gen_server:start_link({local, Name}, ?MODULE, State, []).

func2callback({_, _} = LoopFunc) ->
  func2callback([LoopFunc, undefined, undefined, undefined]);
func2callback([LoopFunc]) ->
  func2callback([LoopFunc, undefined, undefined, undefined]);
func2callback([LoopFunc, CloseFunc]) ->
  func2callback([LoopFunc, CloseFunc, undefined, undefined]);
func2callback([LoopFunc, CloseFunc, RegFunc]) ->
  func2callback([LoopFunc, CloseFunc, RegFunc, undefined]);
func2callback([LoopFunc, CloseFunc, RegFunc, RejectFunc]) ->
  #callback{loop = LoopFunc, close = CloseFunc, reg = RegFunc, reject = RejectFunc}.

init(State = #state{name = Name, port = Port, accept_num = AcceptNum, callback = Callback, tcp_options = TcpOptions}) ->
  process_flag(trap_exit, true),
  ?LOG_INFO("~p initializing port ~p", [Name, Port]),
  reg_func(Callback),
  case gen_tcp:listen(Port, TcpOptions) of
    {ok, ListenSock} ->
      ?LOG_INFO("~p,socket listen success at port ~p", [Name, Port]),
      State2 = State#state{listen_sock = ListenSock},
      start_accepters(AcceptNum, State2),
      {ok, State2};
    {error, Why} ->
      ?LOG_ERROR("~p,socket listen fail at port ~p with ~p", [Port, Why]),
      {stop, Why}
  end.

handle_call({conn_accept, Pid, Socket}, _, #state{conn = Conn, conn_max = MaxConn, sock_map = M} = State) ->
  case Conn < MaxConn of
    true ->
      Ref = erlang:monitor(process, Pid),
      M2 = M#{Ref => {Pid, Socket}},
      {reply, true, State#state{conn = Conn + 1, sock_map = M2}};
    false ->
      {reply, false, State}
  end;
handle_call(_Msg, _Caller, State) ->
  {noreply, State}.

handle_cast(accept_new, State) ->
  proc_lib:spawn(fun() -> accepter(State) end),
  {noreply, State};
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({'DOWN', Ref, _Type, _Object, _Info}, #state{sock_map = M, name = Name, conn = Con} = State) ->
  erlang:demonitor(Ref, [flush]),
  case M of
    #{Ref := {P, Socket}} ->
      ?LOG_INFO("~p, pid ~p, socket ~ts disconnect.", [Name, P, Socket]),
      M2 = maps:remove(Ref, M),
      {noreply, State#state{sock_map = M2, conn = Con - 1}};
    #{} ->
      {noreply, State}
  end;
handle_info(_Msg, State) ->
  {noreply, State}.

code_change(_OldVersion, Library, _Extra) ->
  {ok, Library}.

terminate(_Reason, #state{name = Name, callback = Callback}) ->
  ?LOG_INFO("~p stopping ~n", [Name]),
  close_func(Callback),
  ok.

% 注册函数执行
reg_func(#callback{reg = {M, F}}) -> M:F();
reg_func(_) -> ok.

loop_func(#callback{loop = {M, F}}, Socket) ->
  try
    M:F(Socket)
  catch
    ?EXCEPTION(Class, Reason, Stacktrace) ->
      gen_tcp:close(Socket),
      ?LOG_ERROR("tcp loop fail, stacktrace: ~p, class: ~p, reason: ~p ~n", [?GET_STACK(Stacktrace), Class, Reason])
  end;
loop_func(_, Socket) ->
  gen_tcp:close(Socket).

reject_func(#callback{reject = {M, F}}, Socket) ->
  M:F(Socket),
  gen_tcp:close(Socket);
reject_func(_, Socket) ->
  gen_tcp:close(Socket).

close_func(#callback{close = {M, F}}) -> M:F();
close_func(_) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_accepters(Num, _State) when Num < 1 ->
  ok;
start_accepters(Num, State) ->
  proc_lib:spawn(fun() -> accepter(State) end),
  start_accepters(Num - 1, State).

accepter(#state{name = Name, listen_sock = ListenSock, callback = Callback}) ->
  case gen_tcp:accept(ListenSock) of
    {ok, Socket} ->
      Self = self(),
      SockStr = socket_ip_str(Socket),
      ?LOG_INFO("pid:~p,accept socket ~ts", [Self, SockStr]),
      gen_server:cast(Name, accept_new),
      case gen_server:call(Name, {conn_accept, Self, SockStr}) of
        true ->
          loop_func(Callback, Socket);
        false ->
          reject_func(Callback, Socket)
      end;
    Err ->
      ?LOG_ERROR("accept socket fail with ~p", [Err]),
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