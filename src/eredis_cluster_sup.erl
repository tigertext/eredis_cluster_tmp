%% @private
-module(eredis_cluster_sup).
-behaviour(supervisor).

%% Internal API.
-export([start_link/1, get_pool_sup/1]).

%% Supervisor.
-export([init/1]).

-spec start_link(Name :: atom()) -> {ok, pid()}.
start_link(Name) ->
    supervisor:start_link(?MODULE, Name).

get_pool_sup(ClusterSup) ->
    [Pid] = [Pid || {eredis_cluster_pool, Pid, _, _}
                        <- supervisor:which_children(ClusterSup)],
    true = is_pid(Pid),
    Pid.

-spec init(Name :: atom())
          -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init(Name) ->
    Procs = [{eredis_cluster_pool,
              {eredis_cluster_pool, start_link, []},
              permanent, 5000, supervisor, [eredis_cluster_pool]},
             {eredis_cluster_monitor,
              {eredis_cluster_monitor, start_link, [Name]},
              permanent, 5000, worker, [eredis_cluster_monitor]}
            ],
    {ok, {{rest_for_one, 1, 5}, Procs}}.
