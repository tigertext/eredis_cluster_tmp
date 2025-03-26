%% @doc This module manages the slot mapping. In a Redis cluster, each key
%% belongs to a slot and each slot belongs to a Redis master node.
%%
%% This module is mainly internal, but some functions are documented and may be
%% useful for advanced scenarios.
%%
%% @see eredis_cluster
-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% Internal API.
-export([start_link/1]).
-export([connect/3, disconnect/2]).
-export([refresh_mapping/2, async_refresh_mapping/2]).
-export([get_state/1, get_state_version/1]).
-export([get_pool_by_slot/1, get_pool_by_slot/2]).
-export([get_all_pools/0, get_all_pools/1]).
-export([get_cluster_slots/1, get_cluster_nodes/1]).
-export([get_replicas_for_slot/1, get_replicas_for_slot/2]).

%% Public API (backward compat).
-export([get_cluster_slots/0, get_cluster_nodes/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").

-record(state, {
    init_nodes   = [] :: [#node{}],
    slots_maps   = {} :: tuple(), %% whose elements are #slots_map{}
    node_options = [] :: options(),
    version      = 0  :: integer(),
    slots_table       :: ets:tid() | undefined,
    pool_sup          :: pid() | undefined
}).

-define(cluster_state_table(Cluster), Cluster).
-define(cluster_process(Cluster), Cluster).

%% API.
%% @private
-spec start_link(Cluster :: atom()) -> {ok, pid()}.
start_link(Cluster) ->
    gen_server:start_link({local, Cluster}, ?MODULE, Cluster, []).

%% @private
connect(Cluster, InitServers, Options) ->
    gen_server:call(?cluster_process(Cluster), {connect, InitServers, Options}).

%% @private
disconnect(Cluster, PoolNodes) ->
    gen_server:call(?cluster_process(Cluster), {disconnect, PoolNodes}).

%% @private
refresh_mapping(Cluster, Version) ->
    gen_server:call(?cluster_process(Cluster), {reload_slots_map, Version}).

%% @private
async_refresh_mapping(Cluster, Version) ->
    gen_server:cast(?cluster_process(Cluster), {reload_slots_map, Version}).

%% @private
-spec get_state(Cluster :: atom()) -> #state{}.
get_state(Cluster) ->
    case ets:lookup(?cluster_state_table(Cluster), cluster_state) of
        [{cluster_state, State}] ->
            State;
        [] ->
            #state{}
    end.

%% @private
get_state_version(State) ->
    State#state.version.

%% @doc Returns the connection pools for all Redis nodes in the default cluster.
%%
%% This is useful for commands to a specific node using `qn/2' and
%% `transaction/2'.
%% @see qn/2
%% @see transaction/2
%% @end
%% =============================================================================
-spec get_all_pools() -> [atom()].
get_all_pools() ->
    get_all_pools(?default_cluster).

%% @doc Returns the connection pools for all Redis nodes in a named cluster.
-spec get_all_pools(Cluster :: atom()) -> [atom()].
get_all_pools(Cluster) ->
    State = get_state(Cluster),
    SlotsMaps = tuple_to_list(State#state.slots_maps),
    %% Get all unique pools from both master and replica nodes
    lists:usort([Node#node.pool || 
                    SlotsMap <- SlotsMaps,
                    Node <- [SlotsMap#slots_map.node],
                    Node =/= undefined,
                    Node#node.pool =/= undefined]).

%% =============================================================================
%% @private
%% @doc Get cluster pool by slot.
%% @end
%% =============================================================================
-spec get_pool_by_slot(Slot :: integer()) ->
    {PoolName :: atom() | undefined, Version :: integer()}.
get_pool_by_slot(Slot) ->
    State = get_state(?default_cluster),
    get_pool_by_slot(Slot, State).

%% @private
-spec get_pool_by_slot(Slot :: integer(), State :: #state{}) ->
    {PoolName :: atom() | undefined, Version :: integer()}.
get_pool_by_slot(Slot, State) ->
    try
        [{_, Index}] = ets:lookup(State#state.slots_table, Slot),
        SlotsMap = element(Index, State#state.slots_maps),
        if
            SlotsMap#slots_map.node =/= undefined ->
                {SlotsMap#slots_map.node#node.pool, State#state.version};
            true ->
                {undefined, State#state.version}
        end
    catch
        _:_ ->
            {undefined, State#state.version}
    end.

%% =============================================================================
%% @doc Connect to a init node and get the slot distribution of nodes.
%% @end
%% =============================================================================
-spec reload_slots_map(State::#state{}) -> NewState::#state{}.
reload_slots_map(State) ->
    OldSlotsMaps = tuple_to_list(State#state.slots_maps),

    Options = get_current_options(State),
    ClusterSlots = get_cluster_slots(State, Options),
    NewSlotsMaps = parse_cluster_slots(ClusterSlots, Options),
    %% Find old slots_maps with nodes still in use.
    CommonInOldMap = lists:flatmap(
                       fun(#slots_map{node = Node} = OldElem) ->
                               [OldElem || Elem <- NewSlotsMaps,
                                           Elem#slots_map.node#node.address == Node#node.address,
                                           Elem#slots_map.node#node.port    == Node#node.port,
                                           Elem#slots_map.node#node.options == Node#node.options]
                       end, OldSlotsMaps),

    %% Disconnect non-used nodes
    RemovedFromOldMap = remove_list_elements(OldSlotsMaps, CommonInOldMap),
    PoolSup = State#state.pool_sup,
    [close_connection(PoolSup, SlotsMap) || SlotsMap <- RemovedFromOldMap],

    %% Connect to new nodes
    ConnectedSlotsMaps = connect_all_slots(State#state.pool_sup, NewSlotsMaps),
    create_slots_cache(State#state.slots_table, ConnectedSlotsMaps),
    NewState = State#state{
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        version = State#state.version + 1
    },

    Cluster = this_cluster(),
    true = ets:insert(?cluster_state_table(Cluster),
                      [{cluster_state, NewState}]),
                      
    NewState.

%% =============================================================================
%% @doc Removes all elements (including duplicates) of Ys from Xs.
%% Xs and Ys can be unordered and contain duplicates.
%% @end
%% =============================================================================
-spec remove_list_elements(Xs::[term()], Ys::[term()]) -> [term()].
remove_list_elements(Xs, Ys) ->
    Set = gb_sets:from_list(Ys),
    [E || E <- Xs, not gb_sets:is_element(E, Set)].

%% =============================================================================
%% @doc Get cluster slots information.
%% @end
%% =============================================================================
-spec get_cluster_slots() -> [[bitstring() | [bitstring()]]].
get_cluster_slots() ->
    get_cluster_slots(?default_cluster).

%% @private
-spec get_cluster_slots(Cluster :: atom()) -> [[bitstring() | [bitstring()]]].
get_cluster_slots(Cluster) ->
    State = get_state(Cluster),
    Options = get_current_options(State),
    get_cluster_slots(State, Options).

%% @private
get_cluster_slots(State, Options) ->
    Query = ["CLUSTER", "SLOTS"],
    FailFn = fun get_cluster_slots_from_single_node/1,
    get_cluster_info(State, Options, Query, FailFn).

%% =============================================================================
%% @doc Get cluster nodes information.
%% Returns a list of node elements, each in the form:
%%
%% <pre>[id, ip:port@cport, flags, master, ping-sent, pong-recv, config-epoch, link-state, Slot1, ..., SlotN]
%% </pre>
%%
%% See: https://redis.io/commands/cluster-nodes#serialization-format
%% @end
%% =============================================================================
-spec get_cluster_nodes() -> [[bitstring()]].
get_cluster_nodes() ->
    get_cluster_nodes(?default_cluster).

-spec get_cluster_nodes(Cluster :: atom()) -> [[bitstring()]].
get_cluster_nodes(Cluster) ->
    State = get_state(Cluster),
    Options = get_current_options(State),
    get_cluster_nodes(State, Options).

%% @private
-spec get_cluster_nodes(State :: #state{}, Options :: options()) -> [[bitstring()]].
get_cluster_nodes(State, Options) ->
    Query = ["CLUSTER", "NODES"],
    FailFn = fun(_Node) -> "" end, %% No default data to use when query fails
    ClusterNodes = get_cluster_info(State, Options, Query, FailFn),
    %% Parse result into list of element lists
    NodesInfoList = binary:split(iolist_to_binary(ClusterNodes), <<"\n">>, [global, trim]),
    lists:foldl(fun(Node, Acc) ->
                        Acc ++ [binary:split(Node, <<" ">>, [global, trim])]
                end, [], NodesInfoList).

%% =============================================================================
%% @private
%% @doc Fetch cluster information from an already connected node or from an init
%%      node if no nodes are connected. Throws an exception if everything fails.
%% @end
%% =============================================================================

-spec get_cluster_info(State :: #state{}, Options :: options(), Query :: list(),
                       FailFn :: fun((#node{}) -> iodata())) ->
          iodata().
get_cluster_info(State, Options, Query, FailFn) ->
    %% First try to use existing connections in pools found in the slot map
    case get_cluster_info_from_existing_pools(State#state.slots_maps, Options,
                                              Query, FailFn) of
        {ok, Result} ->
            Result;
        _Error ->
            %% No usable pool connected. Connect to init nodes.
            get_cluster_info_from_init_nodes(State#state.init_nodes, Options,
                                             Query, FailFn, [])
    end.

-spec get_cluster_info_from_existing_pools(SlotMaps :: tuple(),
                                           Options  :: options(),
                                           Query    :: list(),
                                           FailFn   :: fun((#node{}) -> list())) ->
          {ok, list()} | {error, no_connection}.
get_cluster_info_from_existing_pools(SlotMaps, Options, Query, FailFn) ->
    get_cluster_info_from_existing_pools(SlotMaps, Options, Query, FailFn, {1, #{}}).

get_cluster_info_from_existing_pools(SlotMaps, Options, Query, FailFn, SlotMapIterator) ->
    case next_node_in_slots_maps(SlotMaps, Options, SlotMapIterator) of
        {ok, Node, NewSlotMapIterator} ->
            Transaction = fun(Connection) ->
                                  get_cluster_info_from_connection(Connection, Query, FailFn, Node)
                          end,
            try
                {ok, _Result} = poolboy:transaction(Node#node.pool, Transaction)
            catch
                _:_ ->
                    get_cluster_info_from_existing_pools(SlotMaps, Options, Query, FailFn, NewSlotMapIterator)
            end;
        none ->
            {error, no_connection}
    end.

%% Get next node from a tuple of slots maps, using an iterator tuple to remember what's next
-spec next_node_in_slots_maps(SlotsMaps :: tuple(),
                              Options   :: options(),
                              Iterator  :: {Index             :: pos_integer(),
                                            AttemptedPoolsMap :: #{atom() => true}}) ->
          {ok, Node :: #node{}, NextIterator :: {NextIndex            :: pos_integer(),
                                                 NewAttemptedPoolsMap :: #{atom() => true}}} |
          none.
next_node_in_slots_maps(SlotsMaps, Options, {Index, AttemptedPoolsMap})
  when Index =< tuple_size(SlotsMaps) ->
    SlotsMap = element(Index, SlotsMaps),
    case SlotsMap of
        #slots_map{node = Node = #node{options = Options,
                                       pool    = Pool}} ->
            case maps:is_key(Pool, AttemptedPoolsMap) of
                true ->
                    %% This pool has been returned before. Try next.
                    next_node_in_slots_maps(SlotsMaps, Options, {Index + 1, AttemptedPoolsMap});
                false ->
                    %% It's usable.
                    NextIterator = {Index + 1, AttemptedPoolsMap#{Pool => true}},
                    {ok, Node, NextIterator}
            end;
        _NotUsable ->
            %% Options mismatch or node not a record. Try next.
            next_node_in_slots_maps(SlotsMaps, Options, {Index + 1, AttemptedPoolsMap})
    end;
next_node_in_slots_maps(_SlotsMaps, _Options, _Iterator) ->
    none.

%% Connect to an init node, fetch cluster info and disconnect again
get_cluster_info_from_init_nodes([], _Options, _Query, _FailFn, ErrorList) ->
    throw({reply, {error, {cannot_connect_to_cluster, ErrorList}}, #state{}});
get_cluster_info_from_init_nodes([Node|Nodes], Options, Query, FailFn, ErrorList) ->
    case safe_eredis_start_link(Node#node.address, Node#node.port, Options) of
        {ok, Connection} ->
            try get_cluster_info_from_connection(Connection, Query, FailFn, Node) of
                {ok, Result} ->
                    Result;
                Reason ->
                    get_cluster_info_from_init_nodes(Nodes, Options, Query, FailFn,
                                                     [{Node, Reason} | ErrorList])
            after
                eredis:stop(Connection)
            end;
        Reason ->
            get_cluster_info_from_init_nodes(Nodes, Options, Query, FailFn,
                                             [{Node, Reason} | ErrorList])
    end.

-spec get_cluster_info_from_connection(Connection :: pid(),
                                       Query      :: list(),
                                       FailFn     :: fun((#node{}) -> list()),
                                       Node       :: #node{}) ->
          ClusterInfo :: redis_simple_result().
get_cluster_info_from_connection(Connection, Query, FailFn, Node) ->
    try eredis:q(Connection, Query) of
        {ok, ClusterInfo} ->
            {ok, ClusterInfo};
        {error, <<"ERR unknown command 'CLUSTER'">>} ->
            {ok, FailFn(Node)};
        {error, <<"ERR This instance has cluster support disabled">>} ->
            {ok, FailFn(Node)};
        OtherError ->
            OtherError
    catch
        exit:{timeout, {gen_server, call, _}} ->
            {error, timeout}
    end.

-spec get_cluster_slots_from_single_node(#node{}) ->
    [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots(ClusterInfo::[[bitstring() | [bitstring()]]],
                          Options::options()) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo, Options) ->
    SlotsMaps = parse_cluster_slots(ClusterInfo, 1, []),
    %% Save current options in each new SlotsMaps
    [SlotsMap#slots_map{node=SlotsMap#slots_map.node#node{options = Options}} ||
                       SlotsMap <- SlotsMaps].

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | Replicas]] | T], Index, Acc) ->
    %% Create master node
    MasterNode = #node{
        address = binary_to_list(Address),
        port = binary_to_integer(Port),
        role = master
    },
    MasterSlotsMap = #slots_map{
        index = Index,
        start_slot = binary_to_integer(StartSlot),
        end_slot = binary_to_integer(EndSlot),
        node = MasterNode
    },
    
    %% Create replica nodes
    ReplicaSlotsMaps = lists:map(
        fun({[RAddress, RPort | _], RIndex}) ->
            ReplicaNode = #node{
                address = binary_to_list(RAddress),
                port = binary_to_integer(RPort),
                role = replica
            },
            #slots_map{
                index = RIndex,
                start_slot = binary_to_integer(StartSlot),
                end_slot = binary_to_integer(EndSlot),
                node = ReplicaNode
            }
        end,
        lists:zip(Replicas, lists:seq(Index + 1, Index + length(Replicas)))
    ),
    
    parse_cluster_slots(T, Index + length(Replicas) + 1, [MasterSlotsMap | ReplicaSlotsMaps] ++ Acc);
parse_cluster_slots([], _Index, Acc) ->
    lists:reverse(Acc).

%% =============================================================================
%% @doc Collect options set via application configs or in connect/2
%% @end
%% =============================================================================
-spec get_current_options(State::#state{}) -> options().
get_current_options(State) ->
    Env = application:get_all_env(eredis_cluster),
    lists:ukeysort(1, State#state.node_options ++ Env).

%%%------------------------------------------------------------
-spec close_connection_with_nodes(PoolSup :: pid(),
                                  SlotsMaps :: [#slots_map{}],
                                  Pools :: [atom()]) -> [#slots_map{}].
%%%
%%% Close the connection related to specified Pool node.
%%%------------------------------------------------------------
close_connection_with_nodes(PoolSup, SlotsMaps, Pools) ->
    lists:foldl(fun(Map, AccMap) ->
                        case lists:member(Map#slots_map.node#node.pool,
                                          Pools) of
                            true ->
                                close_connection(PoolSup, Map),
                                AccMap;
                            false ->
                                [Map|AccMap]
                        end
                end, [], SlotsMaps).

-spec close_connection(pid(), #slots_map{}) -> ok.
close_connection(PoolSup, SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(PoolSup, Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(pid(), #node{}) -> #node{} | undefined.
connect_node(PoolSup, Node) ->
    case eredis_cluster_pool:create(PoolSup,
                                    Node#node.address,
                                    Node#node.port,
                                    Node#node.options) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

safe_eredis_start_link(Address, Port, Options) ->
    process_flag(trap_exit, true),
    Result = eredis:start_link(Address, Port, Options),
    process_flag(trap_exit, false),
    Result.

-spec create_slots_cache(ets:tid(), [#slots_map{}]) -> true.
create_slots_cache(SlotsTable, SlotsMaps) ->
  SlotsCache = [[{Index, SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot,
            SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  ets:insert(SlotsTable, SlotsCacheF).

-spec connect_all_slots(pid(), [#slots_map{}]) -> [#slots_map{}].
connect_all_slots(PoolSup, SlotsMapList) ->
    [SlotsMap#slots_map{node = connect_node(PoolSup,
                                            SlotsMap#slots_map.node)} ||
        SlotsMap <- SlotsMapList].

-spec connect_(InitNodes :: [{Address :: string(), Port :: integer()}],
               Options :: options(), State :: #state{}) -> #state{}.
connect_(InitNodes, Options, State) ->
    NewState = State#state{
        init_nodes = [#node{address = A, port = P} || {A, P} <- InitNodes],
        node_options = Options
    },

    reload_slots_map(NewState).

-spec disconnect_(PoolNodes :: [atom()], State :: #state{}) -> #state{}.
disconnect_([], State) ->
    State;
disconnect_(PoolNodes, State) ->
    SlotsMaps = tuple_to_list(State#state.slots_maps),
    PoolSup = State#state.pool_sup,
    Cluster = this_cluster(),

    NewSlotsMaps = close_connection_with_nodes(PoolSup, SlotsMaps, PoolNodes),
    ConnectedSlotsMaps = connect_all_slots(PoolSup, NewSlotsMaps),
    create_slots_cache(State#state.slots_table, ConnectedSlotsMaps),

    NewState = State#state{
                 slots_maps = list_to_tuple(ConnectedSlotsMaps),
                 version = State#state.version + 1
                },
    true = ets:insert(?cluster_state_table(Cluster),
                      [{cluster_state, NewState}]),
    NewState.

%% Returns the name of the cluster handled by the current monitor process
this_cluster() ->
    {registered_name, Name} = process_info(self(), registered_name),
    true = is_atom(Name),
    Name.

%% gen_server.

%% @private
init(Cluster) ->
    ets:new(Cluster, [protected, set, named_table, {read_concurrency, true}]),
    SlotsTab = ets:new(slots, [protected, set, {read_concurrency, true}]),
    gen_server:cast(self(), {async_init, Cluster}),
    {ok, #state{slots_table = SlotsTab}}.

%% @private
handle_call({reload_slots_map, Version}, _From, #state{version=Version} = State) ->
    {reply, ok, reload_slots_map(State)};
handle_call({reload_slots_map, _}, _From, State) ->
    %% Mismatching version. Slots map already reloaded.
    {reply, ok, State};
handle_call({connect, InitServers, Options}, _From, State) ->
    {reply, ok, connect_(InitServers, Options, State)};
handle_call({disconnect, PoolNodes}, _From, State) ->
    {reply, ok, disconnect_(PoolNodes, State)};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

%% @private
handle_cast({async_init, Cluster}, State) ->
    {ok, ClusterSup} = eredis_cluster_sup_sup:lookup_cluster(Cluster),
    PoolSup = eredis_cluster_sup:get_pool_sup(ClusterSup),
    InitNodes = case Cluster of
                    ?default_cluster ->
                        application:get_env(eredis_cluster, init_nodes, []);
                    _Other  ->
                        []
                end,

    %% application env options are read later in callstack
    {noreply, connect_(InitNodes, [], State#state{pool_sup = PoolSup})};
handle_cast({reload_slots_map, Version}, #state{version = Version} = State) ->
    {noreply, reload_slots_map(State)};
handle_cast({reload_slots_map, _OldVersion}, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @doc Get all replica nodes for a given slot
-spec get_replicas_for_slot(Slot :: integer()) -> [PoolName :: atom()].
get_replicas_for_slot(Slot) ->
    State = get_state(?default_cluster),
    get_replicas_for_slot(Slot, State).

-spec get_replicas_for_slot(Slot :: integer(), State :: term()) -> [PoolName :: atom()].
get_replicas_for_slot(Slot, State) ->
    try
        [{_, Index}] = ets:lookup(State#state.slots_table, Slot),
        SlotsMap = element(Index, State#state.slots_maps),
        if
            SlotsMap#slots_map.node =/= undefined ->
                MasterNode = SlotsMap#slots_map.node,
                %% Find all replica nodes for this master
                lists:filtermap(
                    fun(#slots_map{node = Node}) ->
                        case Node#node.role of
                            replica when Node#node.address =/= MasterNode#node.address ->
                                {true, Node#node.pool};
                            _ ->
                                false
                        end
                    end,
                    tuple_to_list(State#state.slots_maps)
                );
            true ->
                []
        end
    catch
        _:_ ->
            []
    end.

%% @doc Parse node role from cluster nodes output
-spec parse_node_role(Role :: binary()) -> master | replica.
parse_node_role(Role) ->
    case binary:split(Role, <<",">>, [global]) of
        [<<"master">> | _] -> master;
        [<<"slave">> | _] -> replica;
        _ -> master  % Default to master if role is unclear
    end.

%% @doc Update node role in slots map
-spec update_node_role(SlotsMap :: #slots_map{}, Role :: binary()) -> #slots_map{}.
update_node_role(SlotsMap, Role) ->
    Node = SlotsMap#slots_map.node,
    SlotsMap#slots_map{node = Node#node{role = parse_node_role(Role)}}.

%% @doc Parse cluster nodes output and update node roles
-spec parse_cluster_nodes(Nodes :: binary(), Options :: options()) -> [#node{}].
parse_cluster_nodes(Nodes, Options) ->
    NodeLines = binary:split(Nodes, <<"\n">>, [global]),
    lists:filtermap(
        fun(Line) ->
            case binary:split(Line, <<" ">>, [global]) of
                [NodeId, IpPort, Flags, MasterId | _] ->
                    [Ip, Port] = binary:split(IpPort, <<":">>),
                    Node = #node{
                        address = binary_to_list(Ip),
                        port = binary_to_integer(Port),
                        options = Options,
                        pool = list_to_atom("eredis_cluster_pool_" ++ binary_to_list(NodeId))
                    },
                    {true, update_node_role(Node, Flags)};
                _ ->
                    false
            end
        end,
        NodeLines
    ).
