%% @private
%%
%% Supervision tree:
%%
%%                  eredis_cluster (application)
%%                        |
%%                eredis_cluster_sup_sup
%%                        |
%%                eredis_cluster_sup (one per cluster)
%%                      /   \
%%     eredis_cluster_pool  eredis_cluster_monitor
%%                   /
%%              poolboy (one per Redis node)
%%                /
%%           eredis (one per pool)
%%
%%
%% How to find processes and ETS tables for a cluster?
%% ---------------------------------------------------
%% cluster name = monitor state table = monitor process name
%% slots table <- monitor state
%% pool sup process <- monitor state
%%                  <- cluster sup which_children
%% cluster sup process <- sup sup which_children (match cluster name)
%%
-module(eredis_cluster_sup_sup).
-behaviour(supervisor).

%% Internal API.
-export([start_link/0, add_cluster/1, remove_cluster/1, lookup_cluster/1]).

%% Supervisor behaviour.
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Adds a cluster in disconnected state. Call cluster_monitor:connect/3 to
%% connect to it.
add_cluster(Name) ->
    ChildSpec = {Name,
                 {eredis_cluster_sup, start_link, [Name]},
                 permanent, 5000, supervisor, [eredis_cluster_sup]},
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, _Child} -> ok;
        {error, {already_started, _Child}} -> ok
    end.

%% Disconnects and removes a cluster from the supervision tree.
remove_cluster(Name) ->
    supervisor:terminate_child(?MODULE, Name),
    supervisor:delete_child(?MODULE, Name),
    ok.

%% Returns a cluster supervisor if the cluster exits and an error otherwise.
lookup_cluster(Name) ->
    case [Pid || {Id, Pid, _, _} <- supervisor:which_children(?MODULE),
                 Id =:= Name] of
        [Pid] ->
            {ok, Pid};
        [] ->
            {error, not_found}
    end.

init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.
