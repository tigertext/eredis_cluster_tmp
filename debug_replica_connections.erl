-module(debug_replica_connections).
-export([test_pool_creation/1, debug_cluster_state/1]).

-include("include/eredis_cluster.hrl").

test_pool_creation(ClusterName) ->
    State = eredis_cluster_monitor:get_state(ClusterName),
    SlotsMaps = tuple_to_list(State#state.slots_maps),
    
    lists:foreach(fun(SlotsMap) ->
        MasterNode = SlotsMap#slots_map.node,
        io:format("Master ~s:~p - Pool: ~p~n", 
                 [MasterNode#node.address, MasterNode#node.port, MasterNode#node.pool]),
        
        lists:foreach(fun(ReplicaNode) ->
            io:format("Replica ~s:~p - Pool: ~p~n", 
                     [ReplicaNode#node.address, ReplicaNode#node.port, ReplicaNode#node.pool])
        end, SlotsMap#slots_map.replica_nodes)
    end, SlotsMaps).

debug_cluster_state(ClusterName) ->
    State = eredis_cluster_monitor:get_state(ClusterName),
    io:format("Pool supervisor: ~p~n", [State#state.pool_sup]),
    io:format("Cluster version: ~p~n", [State#state.version]),
    test_pool_creation(ClusterName).
