-module(elib_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  ChildSpecs = redis_proxy() ++ ndisc_redis() ++ redis_global() ++ mysql_proxy(),
  {ok, {SupFlags, ChildSpecs}}.

%% internal functions

redis_proxy() ->
  case application:get_env(redis) of
    {ok, C} when is_list(C) orelse is_map(C) ->
      [
        #{id => redis_proxy,
          start => {redis_proxy, start_link, [redis_proxy, C]},
          restart => permanent,
          significant => false,
          shutdown => 5000,
          type => worker,
          modules => [redis_proxy]}
      ];
    _ ->
      []
  end.

ndisc_redis() ->
  case application:get_env(cluster_id) of
    {ok, Bin} when is_binary(Bin) ->
      [
        #{id => ndisc_redis,
          start => {ndisc_redis, start_link, [Bin]},
          restart => permanent,
          significant => false,
          shutdown => 5000,
          type => worker,
          modules => [ndisc_redis]}
      ];
    _ ->
      []
  end.

redis_global() ->
  case application:get_env(redis_global) of
    {ok, true} ->
      [
        #{id => redis_global,
          start => {redis_global, start_link, []},
          restart => permanent,
          significant => false,
          shutdown => 5000,
          type => worker,
          modules => [redis_global]}
      ];
    _ ->
      []
  end.

mysql_proxy() ->
  case application:get_env(mysql) of
    {ok, C} when is_list(C) orelse is_map(C) ->
      [
        #{id => mysql_proxy,
          start => {mysql_proxy, start_link, [mysql_proxy, C]},
          restart => permanent,
          significant => false,
          shutdown => 5000,
          type => worker,
          modules => [mysql_proxy]}
      ];
    _ ->
      []
  end.
