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
  [{timetrap, {seconds, 30}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(raft),
  application:set_env(raft, max_heartbeat_timeout, 500),
  application:set_env(raft, min_heartbeat_timeout, 100),
  application:set_env(raft, heartbeat_grace_time, 50),
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
      %join_cluster,
      %new_leader,
      leave_join
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
  ?assertEqual({ok, 2}, raft:command(Pid, {store, test_before_leader, 2})),
  ?assertEqual(2, raft:command(Pid, {get, test_before_leader})),
  wait_until_leader([Pid]),
  ?assertEqual({ok, 1}, raft:command(Pid, {store, test, 1})),
  ?assertEqual(1, raft:command(Pid, {get, test})),
  raft:stop(Pid).

command_cluster(_Config) ->
  {ok, PidA} = raft:start(raft_test_cb),
  {ok, PidB} = raft:start(raft_test_cb),
  wait_until_leader([PidA]),
  wait_until_leader([PidB]),
  ok = raft:join(PidA, PidB),

  ?assertEqual({ok, 2}, raft:command(PidA, {store, test_before_leader_a, 2})),
  ?assertEqual(2, raft:command(PidA, {get, test_before_leader_a})),
  ?assertEqual({ok, 2}, raft:command(PidB, {store, test_before_leader_b, 2})),
  ?assertEqual(2, raft:command(PidB, {get, test_before_leader_b})),
  wait_until_leader([PidA, PidB]),
  ?assertEqual({ok, 1}, raft:command(PidA, {store, test_a, 1})),
  ?assertEqual(1, raft:command(PidA, {get, test_a})),
  ?assertEqual({ok, 1}, raft:command(PidB, {store, test_b, 1})),
  ?assertEqual(1, raft:command(PidB, {get, test_b})),

  ?assertEqual({ok, 1}, raft:command(PidA, {store, test_ab, 1})),
  ?assertEqual(1, raft:command(PidB, {get, test_ab})),

  ?assertEqual({ok, 1}, raft:command(PidB, {store, test_ba, 1})),
  ?assertEqual({ok, 1}, raft:command(PidA, {get, test_ba})),

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
  timer:sleep(10),

  assert_status(Leader, leader, Leader, [Slave1, Leader]),

  ok = raft:join(Leader, Slave2),
  % need to wait a bit to slave2 catch up,
  timer:sleep(10),
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
  timer:sleep(10),
  exit(Leader, kill),

  ?assertEqual(ok, wait_until_leader([Slave1, Slave2])),
  #{leader := NewLeader} = raft:status(Slave2),

  % check able to remove dead node:
  ok = raft:leave(NewLeader, Leader),
  timer:sleep(100),

  assert_status(NewLeader, leader, NewLeader, [Slave1, Slave2]),

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

  ok = raft:leave(Leader, Slave1),
  assert_status(Leader, leader, Leader, [Leader, Slave2]),
  ok = raft:leave(Slave2, Leader), % left leader alone
  wait_until_leader([Leader]),
  io:format(user, "status ~p ~n", [raft:status(Leader)]),
  assert_status(Leader, leader, Leader, [Leader]),

  {ok, Slave3} = raft:start(raft_test_cb),
  {ok, Slave4} = raft:start(raft_test_cb),
  timer:sleep(650),
  wait_until_leader([Slave3]),
  wait_until_leader([Slave4]),
  wait_until_leader([Leader]),

  ok = raft:join(Leader, Slave3),
  wait_until_leader([Leader, Slave3]),

  ok = raft:join(Leader, Slave2),
  wait_until_leader([Leader, Slave3, Slave4]),

  assert_status(Leader, leader, Leader, [Slave4, Slave3, Leader]),
  ?assertEqual(ok, wait_until_leader([Slave1, Slave2])),

  raft:stop(Leader),
  raft:stop(Slave1),
  raft:stop(Slave2),
  raft:stop(Slave3),
  raft:stop(Slave4).

wait_until_leader(Pid) when is_pid(Pid) ->
  wait_until_leader([Pid]);
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