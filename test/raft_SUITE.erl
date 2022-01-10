%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_SUITE).
-author("Peter Tihanyi").

%% API
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------
suite() ->
  [{timetrap, {seconds, 10}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(raft),
  %application:set_env(raft, max_heartbeat_timeout, 2500),
  %application:set_env(raft, min_heartbeat_timeout, 500),
  %application:set_env(raft, heartbeat_grace_time, 300),
  logger:set_primary_config(level, debug),
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
  application:stop(raft),
  ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
  ok.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
  ok.

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%%--------------------------------------------------------------------
groups() ->
  [
    {parallel_group, [parallel, shuffle],
     [%instant_leader_when_alone,
      %command_alone,
      %command_cluster,
      join_cluster
      %new_leader,
      %leave_join
    ]}
  ].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%%--------------------------------------------------------------------
all() ->
  [ {group, parallel_group}
  ].

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------
instant_leader_when_alone(_Config) ->
  {ok, Pid} = raft:start(raft_test_cb),
  wait_until_leader([Pid]),
  ?assertMatch(#{role := leader,
                 leader := Pid,
                 cluster_members := [Pid]}, raft:status(Pid)),
  raft:stop(Pid).

command_alone(_Config) ->
  {ok, Pid} = raft:start(raft_test_cb),
  ?assertEqual(2, raft:command(Pid, {store, test_before_leader, 2})),
  ?assertEqual(2, raft:command(Pid, {get, test_before_leader})),
  wait_until_leader([Pid]),
  ?assertEqual(1, raft:command(Pid, {store, test, 1})),
  ?assertEqual(1, raft:command(Pid, {get, test})),
  raft:stop(Pid).

command_cluster(_Config) ->
  {ok, PidA} = raft:start(raft_test_cb),
  {ok, PidB} = raft:start(raft_test_cb),
  wait_until_leader([PidA]),
  wait_until_leader([PidB]),
  ok = raft:join(PidA, PidB),

  ?assertEqual(2, raft:command(PidA, {store, test_before_leader_a, 2})),
  ?assertEqual(2, raft:command(PidA, {get, test_before_leader_a})),
  ?assertEqual(2, raft:command(PidB, {store, test_before_leader_b, 2})),
  ?assertEqual(2, raft:command(PidB, {get, test_before_leader_b})),
  wait_until_leader([PidA, PidB]),
  ?assertEqual(1, raft:command(PidA, {store, test_a, 1})),
  ?assertEqual(1, raft:command(PidA, {get, test_a})),
  ?assertEqual(1, raft:command(PidB, {store, test_b, 1})),
  ?assertEqual(1, raft:command(PidB, {get, test_b})),

  ?assertEqual(1, raft:command(PidA, {store, test_ab, 1})),
  ?assertEqual(1, raft:command(PidB, {get, test_ab})),

  ?assertEqual(1, raft:command(PidB, {store, test_ba, 1})),
  ?assertEqual(1, raft:command(PidA, {get, test_ba})),

  raft:stop(PidA),
  raft:stop(PidB).

join_cluster(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),
  wait_until_leader([Leader]),
  wait_until_leader([Slave1]),
  wait_until_leader([Slave2]),

  ok = raft:join(Leader, Slave1),
  assert_status(Leader, leader, Leader, [Slave1, Leader]),

  ok = raft:join(Leader, Slave2),
  io:format(user, "Status ~p", [raft:status(Slave2)]),
  assert_status(Leader, leader, Leader, [Slave1, Slave2, Leader]),
  assert_status(Slave1, follower, Leader, [Slave1, Slave2, Leader]),
  assert_status(Slave2, follower, Leader, [Slave1, Slave2, Leader]),

  raft:stop(Leader),
  raft:stop(Slave1),
  raft:stop(Slave2).

new_leader(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),
  wait_until_leader([Leader]),
  wait_until_leader([Slave1]),
  wait_until_leader([Slave2]),

  ok = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  assert_status(Leader, leader, Leader, [Leader, Slave1, Slave2]),
  exit(Leader, kill),
  ?assertEqual(ok, wait_until_leader([Slave1, Slave2])),
  #{leader := NewLeader} = raft:status(Slave2),
  exit(NewLeader, kill),
  ct:pal("newl: ~p, old l: ~p Cluster: ~p", [NewLeader, Leader, [Slave2, Slave1]]),
  [LastNode] = [Slave1, Slave2] -- [NewLeader],
  ?assertEqual(ok, wait_until_leader([LastNode])),

  raft:stop(Leader),
  raft:stop(Slave1),
  raft:stop(Slave2).

leave_join(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),
  wait_until_leader([Leader]),
  wait_until_leader([Slave1]),
  wait_until_leader([Slave2]),

  ok = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  assert_status(Leader, leader, Leader, [Leader, Slave1, Slave2]),

  io:format(user, "~n** Cluster formed! **~n", []),

  ok = raft:leave(Leader, Slave1),
  assert_status(Leader, leader, Leader, [Leader, Slave2]),
  io:format(user, "~n** Slave1 leaved! **~n", []),

  ok = raft:leave(Slave2, Leader),
  io:format(user, "~n** Slave2 called to leave leader leaved! **~n", []),
  wait_until_leader([Leader]),
  assert_status(Leader, leader, Leader, [Leader]),

  io:format(user, "~n** leader leaved! **~n", []),

  ok = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  assert_status(Leader, leader, Leader, [Leader, Slave1, Slave2]),

  io:format(user, "~n** Cluster re formed! **~n", []),

  ok = raft:leave(Slave2, Leader),
  io:format(user, "~n** Leader asked to leve by Slave2! **~n", []),
  wait_until_leader([Leader]),
  assert_status(Leader, leader, Leader, [Leader]),
  io:format(user, "~n** Slave 2 leaved! **~n", []),

  ?assertEqual(ok, wait_until_leader([Slave1, Slave2])),

  raft:stop(Leader),
  raft:stop(Slave1),
  raft:stop(Slave2).

wait_until_leader(Pids) ->
  wait_until_leader(Pids, 2500).

wait_until_leader([], _Max) ->
  ok;
wait_until_leader(_Pids, 0) ->
  timeout;
wait_until_leader([Pid | Rem] = Pids, Max) ->
  case raft:status(Pid) of
    #{role := leader} when Pids =:= [Pid] ->
      ok;
    #{role := leader} ->
      wait_until_leader(Rem ++ [Pid], Max);
    #{leader := Leader} ->
      case lists:member(Leader, Pids) of
        true ->
          wait_until_leader(Rem, Max);
        _ ->
          timer:sleep(10),
          wait_until_leader(Pids, Max-1)
      end
  end.

assert_status(Server, ExpectedState, ExpectedLeader, ExpectedClusterMembers) ->
  #{role := ServerState,
    leader := Leader,
    cluster_members := ClusterMembers} = raft:status(Server),
  ?assertEqual(ExpectedState, ServerState),
  ?assertEqual(ExpectedLeader, Leader),
  ?assertEqual(lists:usort(ExpectedClusterMembers), lists:usort(ClusterMembers)).