%% @private
-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/4]).
-export([get_existing_pool/2]).
-export([stop/2]).
-export([transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

-spec create(PoolSup::pid(), Host::string(), Port::integer(), options()) ->
    {ok, PoolName::atom()} | {error, PoolName::atom()}.
create(PoolSup, Host, Port, Options) ->
    PoolName = get_name(Host, Port),

    case whereis(PoolName) of
        undefined ->
            EredisOptions = [{K, V} || {K, V} <- Options,
                                       K =/= pool_size,
                                       K =/= pool_max_overflow],
            WorkerArgs = [{host, Host}, {port, Port}] ++ EredisOptions,
            Size = proplists:get_value(pool_size, Options, 10),
            MaxOverflow = proplists:get_value(pool_max_overflow, Options, 0),

            PoolArgs = [{name, {local, PoolName}},
                        {worker_module, eredis},
                        {size, Size},
                        {max_overflow, MaxOverflow}],

            ChildSpec = poolboy:child_spec(PoolName, PoolArgs, WorkerArgs),
            {Result, _} = supervisor:start_child(PoolSup, ChildSpec),
            {Result, PoolName};
        _ ->
            {ok, PoolName}
    end.

-spec get_existing_pool(Host :: string() | binary(),
                        Port :: inet:port_number()) ->
          {ok, Pool :: atom()} | {error, no_pool}.
get_existing_pool(Host, Port) when is_binary(Host) ->
    get_existing_pool(binary_to_list(Host), Port);
get_existing_pool(Host, Port) when is_list(Host) ->
    Pool = get_name(Host, Port),
    case whereis(Pool) of
        Pid when is_pid(Pid) ->
            {ok, Pool};
        _NoPid ->
            {error, no_pool}
    end.

-spec transaction(PoolName::atom(), fun((Worker::pid()) -> redis_result())) ->
    redis_result().
transaction(PoolName, Transaction) ->
    try
        poolboy:transaction(PoolName, Transaction)
    catch
        exit:{timeout, _GenServerCall} ->
            %% Poolboy checkout timeout, but the pool is consistent.
            {error, pool_busy};
        exit:_ ->
            %% Pool doesn't exist? Refresh mapping solves this.
            {error, no_connection}
    end.

-spec stop(PoolSup :: pid(), PoolName :: atom()) -> ok.
stop(PoolSup, PoolName) ->
    supervisor:terminate_child(PoolSup, PoolName),
    supervisor:delete_child(PoolSup, PoolName),
    ok.

-spec get_name(Host::string(), Port::integer()) -> PoolName::atom().
get_name(Host, Port) ->
    list_to_atom(Host ++ "#" ++ integer_to_list(Port)).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec init([])
          -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.
