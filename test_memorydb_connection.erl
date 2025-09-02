-module(test_memorydb_connection).
-export([test_basic_connection/0, test_replica_routing/0, debug_connection_state/1]).

%% Test script for connecting to MemoryDB cluster with TLS
%% Usage in Erlang shell:
%% c("test_memorydb_connection.erl").
%% test_memorydb_connection:test_basic_connection().

test_basic_connection() ->
    io:format("=== Testing Basic MemoryDB Connection ===~n"),
    
    %% MemoryDB cluster endpoint
    ClusterHost = "clustercfg.xmpp-group-memorydb-env7.ekur4t.memorydb.us-east-1.amazonaws.com",
    ClusterPort = 6379,
    
    %% Configure TLS options for MemoryDB
    TLSOptions = [
        {ssl, true},
        {ssl_options, [
            {verify, verify_none},
            {versions, ['tlsv1.2']},
            {ciphers, ssl:cipher_suites(all, 'tlsv1.2')}
        ]}
    ],
    
    io:format("Connecting to ~s:~p with TLS...~n", [ClusterHost, ClusterPort]),
    
    try
        %% Connect to cluster
        ConnectResult = eredis_cluster:connect([{ClusterHost, ClusterPort}], TLSOptions),
        io:format("Connect result: ~p~n", [ConnectResult]),
        
        case ConnectResult of
            ok ->
                io:format("✓ Connection successful!~n"),
                
                %% Test basic operations
                io:format("~nTesting PING...~n"),
                PingResult = eredis_cluster:q(["PING"]),
                io:format("PING result: ~p~n", [PingResult]),
                
                io:format("~nTesting SET operation...~n"),
                SetResult = eredis_cluster:q(["SET", "test_key", "test_value"]),
                io:format("SET result: ~p~n", [SetResult]),
                
                io:format("~nTesting GET operation...~n"),
                GetResult = eredis_cluster:q(["GET", "test_key"]),
                io:format("GET result: ~p~n", [GetResult]),
                
                %% Clean up
                eredis_cluster:q(["DEL", "test_key"]),
                
                io:format("~n✓ Basic connection test completed successfully~n");
            ConnectError ->
                io:format("✗ Connection failed: ~p~n", [ConnectError])
        end
        
    catch
        ErrorType:Reason ->
            io:format("✗ Exception during connection test: ~p:~p~n", [ErrorType, Reason])
    end.

test_replica_routing() ->
    io:format("=== Testing Replica Routing ===~n"),
    
    try
        %% Enable replica routing
        io:format("Enabling read replicas...~n"),
        application:set_env(eredis_cluster, enable_read_replicas, true),
        
        EnabledStatus = application:get_env(eredis_cluster, enable_read_replicas, false),
        io:format("Read replicas enabled: ~p~n", [EnabledStatus]),
        
        %% Test operations with replica routing
        TestKey = "replica_test_key",
        TestValue = "replica_test_value",
        
        io:format("~nTesting SET (should go to master)...~n"),
        SetResult = eredis_cluster:q(["SET", TestKey, TestValue]),
        io:format("SET result: ~p~n", [SetResult]),
        
        io:format("~nTesting GET (should try replica)...~n"),
        GetResult = eredis_cluster:q(["GET", TestKey]),
        io:format("GET result: ~p~n", [GetResult]),
        
        %% Test multiple read operations
        io:format("~nTesting multiple GET operations...~n"),
        lists:foreach(fun(N) ->
            Key = "test_key_" ++ integer_to_list(N),
            eredis_cluster:q(["SET", Key, "value_" ++ integer_to_list(N)]),
            Result = eredis_cluster:q(["GET", Key]),
            io:format("GET ~s: ~p~n", [Key, Result]),
            eredis_cluster:q(["DEL", Key])
        end, lists:seq(1, 5)),
        
        %% Clean up
        eredis_cluster:q(["DEL", TestKey]),
        
        case GetResult of
            {ok, _} ->
                io:format("~n✓ Replica routing appears to be working~n");
            {error, no_connection} ->
                io:format("~n✗ Replica routing failed with no_connection error~n");
            Other ->
                io:format("~n? Unexpected result: ~p~n", [Other])
        end
        
    catch
        ErrorType:Reason ->
            io:format("Error testing replica routing: ~p:~p~n", [ErrorType, Reason])
    end.

debug_connection_state(ClusterName) ->
    io:format("=== Debugging Connection State for ~p ===~n", [ClusterName]),
    
    try
        %% Get cluster state
        State = eredis_cluster_monitor:get_state(ClusterName),
        io:format("Cluster state retrieved~n"),
        
        %% Extract basic info
        Version = element(6, State),
        PoolSup = element(4, State),
        SlotsMaps = tuple_to_list(element(3, State)),
        
        io:format("Cluster version: ~p~n", [Version]),
        io:format("Pool supervisor: ~p~n", [PoolSup]),
        io:format("Number of slot mappings: ~p~n", [length(SlotsMaps)]),
        
        %% Check first few slot mappings
        io:format("~n=== Slot Mappings (first 3) ===~n"),
        lists:foreach(fun({Index, SlotsMap}) ->
            case Index =< 3 of
                true ->
                    StartSlot = element(2, SlotsMap),
                    EndSlot = element(3, SlotsMap),
                    MasterNode = element(5, SlotsMap),
                    ReplicaNodes = element(6, SlotsMap),
                    
                    io:format("Slots ~p-~p:~n", [StartSlot, EndSlot]),
                    
                    %% Master info
                    MasterAddr = element(2, MasterNode),
                    MasterPort = element(3, MasterNode),
                    MasterPool = element(5, MasterNode),
                    io:format("  Master: ~s:~p -> Pool: ~p~n", [MasterAddr, MasterPort, MasterPool]),
                    
                    %% Replica info
                    case ReplicaNodes of
                        [] ->
                            io:format("  Replicas: None~n");
                        _ ->
                            io:format("  Replicas (~p):~n", [length(ReplicaNodes)]),
                            lists:foreach(fun(ReplicaNode) ->
                                ReplicaAddr = element(2, ReplicaNode),
                                ReplicaPort = element(3, ReplicaNode),
                                ReplicaPool = element(5, ReplicaNode),
                                io:format("    ~s:~p -> Pool: ~p~n", [ReplicaAddr, ReplicaPort, ReplicaPool])
                            end, ReplicaNodes)
                    end;
                false ->
                    ok
            end
        end, lists:zip(lists:seq(1, length(SlotsMaps)), SlotsMaps))
        
    catch
        ErrorType:Reason ->
            io:format("Error debugging connection state: ~p:~p~n", [ErrorType, Reason])
    end.
