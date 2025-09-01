-module(test_replica_support).
-export([test_command_classification/0, test_replica_routing/0]).

-include("include/eredis_cluster.hrl").

test_command_classification() ->
    io:format("Testing command classification...~n"),

    true = eredis_cluster:is_read_command(["GET", "mykey"]),
    true = eredis_cluster:is_read_command([<<"GET">>, <<"mykey">>]),
    true = eredis_cluster:is_read_command(["MGET", "key1", "key2"]),
    true = eredis_cluster:is_read_command(["HGET", "myhash", "field"]),
    true = eredis_cluster:is_read_command(["SCAN", "0"]),
    true = eredis_cluster:is_read_command(["EXISTS", "mykey"]),

    false = eredis_cluster:is_read_command(["SET", "mykey", "value"]),
    false = eredis_cluster:is_read_command(["DEL", "mykey"]),
    false = eredis_cluster:is_read_command(["HSET", "myhash", "field", "value"]),
    false = eredis_cluster:is_read_command(["LPUSH", "mylist", "value"]),
    false = eredis_cluster:is_read_command(["SADD", "myset", "member"]),

    io:format("Command classification tests passed!~n"),
    ok.

test_replica_routing() ->
    io:format("Testing replica routing logic...~n"),

    SlotsMap = #slots_map{
        start_slot = 0,
        end_slot = 5460,
        index = 1,
        node = #node{address = "127.0.0.1", port = 7000, pool = master_pool},
        replica_nodes = [
            #node{address = "127.0.0.1", port = 7001, pool = replica_pool_1},
            #node{address = "127.0.0.1", port = 7002, pool = replica_pool_2}
        ]
    },

    io:format("Created test slots map with master and 2 replicas~n"),
    io:format("Master: ~p~n", [SlotsMap#slots_map.node]),
    io:format("Replicas: ~p~n", [SlotsMap#slots_map.replica_nodes]),

    ok.
