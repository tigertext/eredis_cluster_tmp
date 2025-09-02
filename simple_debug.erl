-module(simple_debug).
-export([debug_replica_state/1, test_replica_pools/1, force_reload_cluster/1]).

%% Simple debugging module that doesn't require includes
%% Works directly with eredis_cluster_monitor functions

debug_replica_state(ClusterName) ->
    io:format("=== Debugging Replica State for ~p ===~n", [ClusterName]),
    
    try
        %% Get the cluster state
        State = eredis_cluster_monitor:get_state(ClusterName),
        io:format("Cluster state retrieved successfully~n"),
        
        %% Extract slots maps (it's a tuple, convert to list)
        SlotsMaps = tuple_to_list(element(3, State)),
        io:format("Found ~p slot mappings~n", [length(SlotsMaps)]),
        
        %% Check each slot mapping
        lists:foreach(fun(SlotsMap) ->
            %% Extract fields manually since we don't have record definitions
            StartSlot = element(2, SlotsMap),
            EndSlot = element(3, SlotsMap),
            MasterNode = element(5, SlotsMap),
            ReplicaNodes = element(6, SlotsMap),
            
            io:format("~nSlots ~p-~p:~n", [StartSlot, EndSlot]),
            
            %% Check master node
            MasterAddress = element(2, MasterNode),
            MasterPort = element(3, MasterNode),
            MasterPool = element(5, MasterNode),
            io:format("  Master: ~s:~p -> Pool: ~p~n", [MasterAddress, MasterPort, MasterPool]),
            
            %% Check replica nodes
            case ReplicaNodes of
                [] ->
                    io:format("  No replica nodes found!~n");
                _ ->
                    io:format("  Replicas (~p):~n", [length(ReplicaNodes)]),
                    lists:foreach(fun(ReplicaNode) ->
                        ReplicaAddress = element(2, ReplicaNode),
                        ReplicaPort = element(3, ReplicaNode),
                        ReplicaPool = element(5, ReplicaNode),
                        io:format("    ~s:~p -> Pool: ~p~n", [ReplicaAddress, ReplicaPort, ReplicaPool])
                    end, ReplicaNodes)
            end
        end, SlotsMaps),
        
        io:format("~n=== Debug Complete ===~n")
        
    catch
        Error:Reason ->
            io:format("Error debugging cluster state: ~p:~p~n", [Error, Reason])
    end.

test_replica_pools(ClusterName) ->
    io:format("=== Testing Replica Pool Connections ===~n"),
    
    try
        %% Test if replica routing is enabled
        EnableReplicas = application:get_env(eredis_cluster, enable_read_replicas, false),
        io:format("Read replicas enabled: ~p~n", [EnableReplicas]),
        
        %% Test basic operations
        TestKey = "debug_test_key",
        TestValue = "debug_test_value",
        
        io:format("~nTesting SET operation...~n"),
        SetResult = eredis_cluster:q(ClusterName, ["SET", TestKey, TestValue]),
        io:format("SET result: ~p~n", [SetResult]),
        
        io:format("~nTesting GET operation...~n"),
        GetResult = eredis_cluster:q(ClusterName, ["GET", TestKey]),
        io:format("GET result: ~p~n", [GetResult]),
        
        %% Clean up
        eredis_cluster:q(ClusterName, ["DEL", TestKey]),
        
        case {SetResult, GetResult} of
            {{ok, <<"OK">>}, {ok, _}} ->
                io:format("~n✓ Both SET and GET operations successful~n");
            {{ok, <<"OK">>}, {error, no_connection}} ->
                io:format("~n✗ SET works but GET fails with no_connection (replica issue)~n");
            _ ->
                io:format("~n? Unexpected results~n")
        end
        
    catch
        Error:Reason ->
            io:format("Error testing pools: ~p:~p~n", [Error, Reason])
    end.

force_reload_cluster(ClusterName) ->
    io:format("=== Forcing Cluster Reload for ~p ===~n", [ClusterName]),
    try
        %% Force a reload of the cluster slots map to trigger connection attempts
        eredis_cluster_monitor:refresh_mapping(ClusterName),
        io:format("Cluster reload triggered - check console for REPLICA_DEBUG messages~n")
    catch
        Error:Reason ->
            io:format("Error forcing cluster reload: ~p:~p~n", [Error, Reason])
    end.
