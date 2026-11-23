%% Read routing helpers that run for every command. No Redis needed.
-module(eredis_cluster_routing_tests).

-include_lib("eunit/include/eunit.hrl").

read_command_test_() ->
    [?_assert(eredis_cluster:is_read_command(["GET", <<"k">>])),
     ?_assert(eredis_cluster:is_read_command([<<"GET">>, <<"k">>])),
     ?_assert(eredis_cluster:is_read_command(["SSCAN", <<"k">>, "0", "COUNT", "10"])),
     %% verbs are case-insensitive, as before
     ?_assert(eredis_cluster:is_read_command(["get", <<"k">>])),
     ?_assert(eredis_cluster:is_read_command([<<"sMembers">>, <<"k">>])),
     ?_assertNot(eredis_cluster:is_read_command(["SET", <<"k">>, <<"v">>])),
     ?_assertNot(eredis_cluster:is_read_command([<<"del">>, <<"k">>])),
     %% a pipeline is not a read command (it goes to the master)
     ?_assertNot(eredis_cluster:is_read_command([["GET", <<"a">>], ["GET", <<"b">>]])),
     ?_assertNot(eredis_cluster:is_read_command([""])),
     ?_assertNot(eredis_cluster:is_read_command([]))].

replica_flag_test_() ->
    {foreach,
     fun() -> application:unset_env(eredis_cluster, enable_read_replicas) end,
     fun(_) -> application:unset_env(eredis_cluster, enable_read_replicas) end,
     [fun() -> ?assertEqual(false, eredis_cluster_monitor:read_replicas_enabled([])) end,
      fun() -> ?assertEqual(true, eredis_cluster_monitor:read_replicas_enabled([{pool_size, 5}, {enable_read_replicas, true}])) end,
      fun() ->
              application:set_env(eredis_cluster, enable_read_replicas, true),
              ?assertEqual(true, eredis_cluster_monitor:read_replicas_enabled([{pool_size, 5}]))
      end,
      fun() ->
              %% the cluster's own option wins over the app env
              application:set_env(eredis_cluster, enable_read_replicas, true),
              ?assertEqual(false, eredis_cluster_monitor:read_replicas_enabled([{enable_read_replicas, false}]))
      end]}.

%% Same answer as the former per-read computation:
%% proplists:get_value(enable_read_replicas, get_current_options(State), Default).
same_as_options_merge_test_() ->
    Cases = [{NodeOptions, Env} || NodeOptions <- [[], [{pool_size, 5}],
                                                   [{enable_read_replicas, true}],
                                                   [{enable_read_replicas, false}, {pool_size, 5}]],
                                   Env <- [unset, true, false]],
    {foreach,
     fun() -> application:unset_env(eredis_cluster, enable_read_replicas) end,
     fun(_) -> application:unset_env(eredis_cluster, enable_read_replicas) end,
     [fun() ->
              case Env of
                  unset -> application:unset_env(eredis_cluster, enable_read_replicas);
                  _ -> application:set_env(eredis_cluster, enable_read_replicas, Env)
              end,
              Merged = lists:ukeysort(1, NodeOptions ++ application:get_all_env(eredis_cluster)),
              Expected = proplists:get_value(enable_read_replicas, Merged, false),
              ?assertEqual({NodeOptions, Env, Expected},
                           {NodeOptions, Env, eredis_cluster_monitor:read_replicas_enabled(NodeOptions)})
      end || {NodeOptions, Env} <- Cases]}.
