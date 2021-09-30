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
  application:ensure_all_started(raft),
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
  [].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%%--------------------------------------------------------------------
all() ->
  [ instant_leader_when_alone,
    join_cluster,
    new_leader,
    leave_join
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
  timer:sleep(10),
  ?assertMatch({leader, _, Pid, [Pid]}, raft:status(Pid)).

join_cluster(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),
  ok = raft:join(Leader, Slave1),
  ?assertMatch({leader, _, Leader, [Leader, Slave1]}, raft:status(Leader)),

  ok = raft:join(Leader, Slave2),
  ?assertMatch({leader, _, Leader, [Leader, Slave1, Slave2]}, raft:status(Leader)),
  ?assertMatch({follower, _, Leader, [Leader, Slave1, Slave2]}, raft:status(Slave1)),
  ?assertMatch({follower, _, Leader, [Leader, Slave1, Slave2]}, raft:status(Slave2)).

new_leader(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),
  ok = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  ?assertMatch({leader, _, Leader, [Leader, Slave1, Slave2]}, raft:status(Leader)),
  exit(Leader, kill),
  ?assertEqual(ok, wait_until_leader([Slave1, Slave2])),
  {_, _, NewLeader, _} = raft:status(Slave2),
  exit(NewLeader, kill),
  ct:pal("newl: ~p, old l: ~p Clueter: ~p", [NewLeader, Leader, [Slave2, Slave1]]),
  [LastNode] = [Slave1, Slave2] -- [NewLeader],
  ?assertEqual(ok, wait_until_leader([LastNode])).

leave_join(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),
  ok = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  ?assertMatch({leader, _, Leader, [Leader, Slave1, Slave2]}, raft:status(Leader)),

  ok = raft:leave(Leader, Slave1),
  ?assertMatch({leader, _, Leader, [Leader, Slave2]}, raft:status(Leader)),

  ok = raft:leave(Slave2, Leader),
  wait_until_leader([Leader]),
  ?assertMatch({leader, _, Leader, [Leader]}, raft:status(Leader)),

  ok = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  ?assertMatch({leader, _, Leader, [Leader, Slave1, Slave2]}, raft:status(Leader)),

  ok = raft:leave(Slave2, Leader),
  wait_until_leader([Leader]),
  ?assertMatch({leader, _, Leader, [Leader]}, raft:status(Leader)),

  ?assertEqual(ok, wait_until_leader([Slave1, Slave2])).


wait_until_leader(Pids) ->
  wait_until_leader(Pids, 2500).

wait_until_leader([], _Max) ->
  ok;
wait_until_leader(_Pids, 0) ->
  timeout;
wait_until_leader([Pid | Rem] = Pids, Max) ->
  case raft:status(Pid) of
    {leader, _Term, _Leader, _Members} when Pids =:= [Pid] ->
      ok;
    {leader, _Term, _Leader, _Members} ->
      wait_until_leader(Rem ++ [Pid], Max);
    {_Type, _Term, Leader, _Members} ->
      case lists:member(Leader, Pids) of
        true ->
          wait_until_leader(Rem, Max);
        _ ->
          timer:sleep(10),
          wait_until_leader(Pids, Max-1)
      end
  end.