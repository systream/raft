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
  application:set_env(raft, max_heartbeat_timeout, 300),
  application:set_env(raft, min_heartbeat_timeout, 100),
  application:set_env(raft, heartbeat_grace_time, 50),
  logger:set_primary_config(level, notice),
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
     [instant_leader_when_alone
      , command_alone
      , command_cluster
      , join_cluster
      %, join_cluster_cross
      , new_leader
      , catch_up
      , cluster_leave
      , able_to_server_with_failure
      , not_able_to_server_without_majority_failure
      , parallel_joins
      , kill_the_leader_under_load
      , with_server_id
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
  wait_until_leader(Pid),
  ?assertMatch(#{role := leader,
                 leader := Pid,
                 cluster_members := [Pid]}, raft:status(Pid)),
  raft:stop(Pid).

command_alone(_Config) ->
  {ok, Pid} = raft:start(raft_test_cb),
  ?assertEqual({error, no_leader}, raft:command(Pid, {store, test_before_leader, 2})),
  ?assertEqual({error, no_leader}, raft:query(Pid, {get, test_before_leader})),
  wait_until_leader(Pid),
  ?assertEqual(ok, raft:command(Pid, {store, test, 1})),
  ?assertEqual(1, raft:query(Pid, {get, test})),
  raft:stop(Pid).

command_cluster(_Config) ->
  {ok, PidA} = raft:start(raft_test_cb),
  {ok, PidB} = raft:start(raft_test_cb),
  wait_until_leader(PidA),
  wait_until_leader(PidB),
  ok = retry_until({raft, join, [PidA, PidB]}),

  ?assertEqual(ok, raft:command(PidA, {store, test_before_leader_a, 2})),
  ?assertEqual(2, raft:query(PidA, {get, test_before_leader_a})),
  ?assertEqual(ok, raft:command(PidB, {store, test_before_leader_b, 2})),
  ?assertEqual(2, raft:query(PidB, {get, test_before_leader_b})),
  wait_until_leader([PidA, PidB]),
  ?assertEqual(ok, raft:command(PidA, {store, test_a, 1})),
  ?assertEqual(1, raft:query(PidA, {get, test_a})),
  ?assertEqual(ok, raft:command(PidB, {store, test_b, 1})),
  ?assertEqual(1, raft:query(PidB, {get, test_b})),

  ?assertEqual(ok, raft:command(PidA, {store, test_ab, 1})),
  ?assertEqual(1, raft:query(PidB, {get, test_ab})),

  ?assertEqual(ok, raft:command(PidB, {store, test_ba, 1})),
  ?assertEqual(1, raft:query(PidA, {get, test_ba})),

  raft:stop(PidA),
  raft:stop(PidB).

join_cluster(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),

  ok = retry_until({raft, join, [Leader, Slave1]}),
  timer:sleep(10),

  assert_status(Leader, leader, Leader, [Slave1, Leader]),

  {error, already_member} = raft:join(Leader, Slave1),
  ok = raft:join(Leader, Slave2),
  % need to wait a bit to slave2 catch up,
  timer:sleep(10),
  assert_status(Leader, leader, Leader, [Slave1, Slave2, Leader]),
  assert_status(Slave1, follower, Leader, [Slave1, Slave2, Leader]),
  assert_status(Slave2, follower, Leader, [Slave1, Slave2, Leader]),

  stop([Leader, Slave1, Slave2]).

join_cluster_cross(_Config) ->
  {ok, Leader} = raft:start(raft_test_cb),
  {ok, Slave1} = raft:start(raft_test_cb),
  {ok, Slave2} = raft:start(raft_test_cb),

  ok = retry_until({raft, join, [Slave1, Leader]}),
  ok = retry_until({raft, join, [Slave2, Leader]}),
  timer:sleep(600),

  #{cluster_members := MembersL} = raft:status(Leader),
  #{cluster_members := MembersS1} = raft:status(Slave1),
  #{cluster_members := MembersS2} = raft:status(Slave2),

  ?assertEqual(MembersL, MembersS1),
  ?assertEqual(MembersL, MembersS2),

  stop([Leader, Slave1, Slave2]).

new_leader(_Config) ->
  [Leader, Slave1, Slave2] = form_cluster(3),
  assert_status(Leader, leader, Leader, [Leader, Slave1, Slave2]),
  exit(Leader, kill),
  % need some time to realise that the leader has died
  timer:sleep(15),

  % check able to remove dead node:
  ok = retry_until({raft, leave, [Slave1, Leader]}, 30),

  stop([Slave1, Slave2]).

catch_up(_Config) ->
  [Leader, Slave1, Slave2] = form_cluster(3),

  [raft:command(Leader, {hash, {test, I rem 5}, I}) || I <- lists:seq(1, 1000)],

  {ok, Slave3} = raft:start(raft_test_cb),
  {ok, Slave4} = raft:start(raft_test_cb),

  ok = retry_until({raft, join, [Leader, Slave3]}, 10),
  wait_to_catch_up_follower([Leader, Slave1, Slave2, Slave3]),
  ?assertEqual(ok, compare_logs([Leader, Slave1, Slave2, Slave3])),
  ?assertEqual(ok, compare_states([Leader, Slave1, Slave2, Slave3])),

  ok = retry_until({raft, join, [Leader, Slave4]}, 10),
  wait_to_catch_up_follower([Leader, Slave1, Slave2, Slave3, Slave4]),
  ?assertEqual(ok, compare_logs([Leader, Slave1, Slave2, Slave3, Slave4])),
  ?assertEqual(ok, compare_states([Leader, Slave1, Slave2, Slave3, Slave4])),

  {ok, Slave5} = raft:start(raft_test_cb),
  {ok, Slave6} = raft:start(raft_test_cb),

  ok = retry_until({raft, join, [Leader, Slave5]}, 10),
  ok = retry_until({raft, join, [Leader, Slave6]}, 10),

  wait_to_catch_up_follower([Leader, Slave1, Slave2, Slave3, Slave4, Slave5, Slave6]),
  ?assertEqual(ok, compare_logs([Leader, Slave1, Slave2, Slave3, Slave4, Slave5, Slave6])),
  ?assertEqual(ok, compare_states([Leader, Slave1, Slave2, Slave3, Slave4, Slave5, Slave6])),

  stop([Leader, Slave1, Slave2, Slave3, Slave4, Slave5, Slave6]).

cluster_leave(_Config) ->
  [Leader, Slave1, Slave2] = Servers = form_cluster(3),

  [raft:command(Leader, {hash, {test, I rem 5}, I}) || I <- lists:seq(1, 100)],

  ok = retry_until({raft, leave, [Slave1, Leader]}),
  EvalFun = fun(Result) ->
              case Result of
                {error, not_member} -> ok;
                {error, last_member_in_the_cluster} -> ok;
                Else -> Else
              end
            end,
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Slave2, Leader]}))),
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Slave2, Slave1]}))),
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Slave2, Slave2]}))),

  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Leader, Leader]}))),
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Leader, Slave1]}))),
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Leader, Slave2]}))),

  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Slave1, Leader]}))),
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Slave1, Slave1]}))),
  ?assertEqual(ok, EvalFun(retry_until({raft, leave, [Slave1, Slave2]}))),

  stop(Servers).

able_to_server_with_failure(_Config) ->
  [Leader, S1 | _] = Servers = form_cluster(3),
  {ok, _} = raft:command(Leader, {hash, test, 1}),
  exit(Leader, kill),
  {ok, _} = retry_until({raft, command, [S1, {hash, test, 2}]}),
  stop(Servers).

not_able_to_server_without_majority_failure(_Config) ->
  [Leader, S1, S2] = Servers = form_cluster(3),
  {ok, _} = raft:command(Leader, {hash, test, 1}),
  exit(Leader, kill),
  exit(S1, kill),
  timer:sleep(1600),
  % both answer can be correct
  case retry_until({raft, command, [S2, {hash, test, 2}]}, 10) of
    {'EXIT', {timeout, _}} -> ok;
    {error, no_leader} -> ok
  end,
  stop(Servers).

parallel_joins(_Config) ->
  {ok, A} = raft:start(raft_test_cb),
  {ok, B} = raft:start(raft_test_cb),
  wait_until_leader(A),
  wait_until_leader(B),

  Self = self(),
  spawn(fun() -> Self ! {result, A, retry_until({raft, join, [A, B]})} end),
  spawn(fun() -> Self ! {result, B, retry_until({raft, join, [B, A]})} end),

  WaitFun = fun(Proc) ->
              receive
                {result, Proc, Result} ->
                  {ok, Result}
              after 1000 ->
                timeout
              end
            end,

  WaitFun(A),
  WaitFun(B),

  #{cluster_members := AClusterMembers} = raft:status(A),
  #{cluster_members := BClusterMembers} = raft:status(A),

  ?assertEqual(lists:usort([A, B]), lists:usort(AClusterMembers)),
  ?assertEqual(lists:usort(BClusterMembers), lists:usort(AClusterMembers)),
  stop([A, B]).

kill_the_leader_under_load(_Config) ->
  Servers = form_cluster(5),
  timer:sleep(5),
  #{leader := Leader} = raft:status(hd(Servers)),
  spawn(fun() -> timer:sleep(10), exit(Leader, kill) end),

  [raft:command(pick_server(Servers), {hash, I, I}) || I <- lists:seq(1, 1000)],

  NewServers = Servers -- [Leader],
  wait_until_leader(NewServers),
  wait_to_catch_up_follower(NewServers),
  ?assertEqual(ok, compare_logs(NewServers)),
  ?assertEqual(ok, compare_states(NewServers)),

  stop(Servers).


with_server_id(_Config) ->
  {ok, Pid} = raft:start(<<"id">>, raft_test_cb),
  stop([Pid]).

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

retry_until(MFA) ->
  retry_until(MFA, 30).

retry_until({M, F, A} = MFA, Times) ->
  case catch apply(M, F, A) of
    {error, no_leader} when Times > 0 ->
      timer:sleep(100),
      retry_until(MFA, Times-1);
    {error, leader_changed} when Times > 0 ->
      timer:sleep(100),
      retry_until(MFA, Times-1);
    {error, already_member} when  M =:= raft andalso F =:= join ->
      ok;
    Else ->
      Else
  end.

compare_logs(Servers) ->
  ct:pal("Comparing logs ~n", []),
  LogRefs = get_log_refs(Servers),
  Leader = determine_leader(Servers),
  LeaderLa = last_log_index(Leader),
  compare_logs(LogRefs, 1, LeaderLa),
  ct:pal("Comparing done ~n", []),
  ok.

compare_logs(Logs, Index, LeaderLa) when Index =< LeaderLa ->
  LogResults = [{Server, raft_log:get(Log, Index)} || {Server, Log} <- Logs],
  case do_compare_term([LogResult || {_, LogResult} <- LogResults]) of
    ok ->
      ok;
    diff ->
      ct:pal("Diff at: ~p -> ~p~n", [Index, LogResults])
  end,
  compare_logs(Logs, Index+1, LeaderLa);
compare_logs(_, _, _) ->
  ok.

compare_states(Servers) ->
  ct:pal("Comparing user states ~n", []),
  UserStates = get_user_states(Servers),
  case do_compare_term([UserState || {_, UserState} <- UserStates]) of
    ok ->
      ok;
    diff ->
      ct:pal("Diff: ~p ~n", [UserStates])
  end,
  ct:pal("Comparing done ~n", []),
  ok.

do_compare_term([Base | Rest]) ->
  compare_term(Rest, Base).

compare_term([], _Base) ->
  ok;
compare_term([Base | Rest], Base) ->
  compare_term(Rest, Base);
compare_term([Diff | _Rest], Base) when Diff =/= Base ->
  diff.

get_log_refs(Servers) ->
  [begin
     {_, State} = sys:get_state(Server),
     {Server, erlang:element(7, State)}
   end || Server <- Servers].

get_user_states(Servers) ->
  [begin
     {_, State} = sys:get_state(Server),
     {Server, erlang:element(3, State)}
   end || Server <- Servers].

last_log_index(Server) ->
  #{log_last_index := La} = raft:status(Server),
  La.

determine_leader([Server | RemServers]) ->
  case raft:status(Server) of
    #{leader := undefined} ->
      timer:sleep(10),
      determine_leader(RemServers);
    #{leader := Leader} ->
      Leader
  end.

wait_to_catch_up_follower(Servers) ->
  Leader = determine_leader(Servers),
  LeaderLa = last_log_index(Leader),
  ct:pal("Leader is ~p and last applied is: ~p~n", [Leader, LeaderLa]),
  Followers = Servers -- [Leader],
  is_behind_check(Followers, LeaderLa, 100),
  ok.

is_behind_check(Followers, LeaderLa, MaxWait) ->
  case is_behind(Followers, LeaderLa, false) of
    false ->
      ct:pal("Finally every follower has catched up~n", []),
      ok;
    true when MaxWait > 0 ->
      timer:sleep(100),
      is_behind_check(Followers, LeaderLa, MaxWait-1);
    true ->
      diff
  end.

is_behind([], _LeaderLa, Result) ->
  Result;
is_behind([Server | Servers], LeaderLa, Result) ->
  LastApplied = last_log_index(Server),
  case LastApplied < LeaderLa of
    true ->
      ct:pal("Server ~p is behind master ~p (~p ~p)~n",
                [Server, LeaderLa-LastApplied, LastApplied, LeaderLa]),
      is_behind(Servers, LeaderLa, true);
    false ->
      is_behind(Servers, LeaderLa, Result)
  end.

form_cluster(MemberCount) ->
  Servers = [begin {ok, Pid} = raft:start(raft_test_cb), Pid end || _ <- lists:seq(1, MemberCount)],
  [ok = wait_until_leader(Pid) || Pid <- Servers],
  [Leader | Slaves] = Servers,
  [ok = retry_until({raft, join, [Leader, Server]}) || Server <- Slaves],
  Servers.

stop(Servers) ->
  [raft:stop(Pid) || Pid <- Servers, erlang:is_process_alive(Pid)],
  ok.

pick_server(Servers) ->
  lists:nth(rand:uniform(length(Servers)), Servers).