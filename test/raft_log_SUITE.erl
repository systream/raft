%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_log_SUITE).
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
  [{ref, raft_log:new([{retention_time, 60}])} | Config].

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
  raft_log:destroy(?config(ref, Config)),
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
  [ stream_empty,
    append_log,
    out_of_range,
    overwrite,
    retention_time,
    retention_time_not_enough_token
  ].

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------
stream_empty(Config) ->
  LogRef = ?config(ref, Config),
  {ok, Data} = wait_for_log_stream(raft_log:stream(LogRef, self()), []),
  ?assertEqual([], Data).

append_log(Config) ->
  Log = ?config(ref, Config),
  Log1 = raft_log:append(<<"test data">>, Log),
  Log2 = raft_log:append(<<"test data2">>, Log1),

  % From the begining
  {ok, Data} = wait_for_log_stream(raft_log:stream(Log, self()), []),
  ?assertEqual([{2,  <<"test data2">>}, {1, <<"test data">>}], Data),

  % at the end
  {ok, Data2} = wait_for_log_stream(raft_log:stream(Log2, self()), []),
  ?assertEqual([], Data2).

out_of_range(Config) ->
  {log_ref, Pid, _LogPos} = Log = ?config(ref, Config),
  _NewLog = lists:foldl(fun raft_log:append/2, Log, lists:seq(1, 10)),

  LogOverLast = {log_ref, Pid, 10000},
  LogOverFirst = {log_ref, Pid, -10},

  ?assertEqual({error, {bad_log_pos, 10000, {1, 10}}}, raft_log:stream(LogOverLast, self())),

  % auto fallback for first known item
  ?assertMatch({ok, _}, raft_log:stream(LogOverFirst, self())).

overwrite(Config) ->
  Log = ?config(ref, Config),
  _LogTen = lists:foldl(fun raft_log:append/2, Log, lists:seq(1, 10)),
  NewLog2 = raft_log:append("test_1", Log),
  NewLog3 = raft_log:append("test_2", NewLog2),
  NewLog4 = raft_log:append("test_3", NewLog3),
  {ok, Data1} = wait_for_log_stream(raft_log:stream(NewLog2, self()), []),
  {ok, Data2} = wait_for_log_stream(raft_log:stream(NewLog4, self()), []),
  ?assertEqual([{3, "test_3"}, {2, "test_2"}], Data1),
  ?assertEqual([], Data2),

  NewLog4 = raft_log:append("test_3_1", NewLog3),
  {ok, Data3} = wait_for_log_stream(raft_log:stream(NewLog2, self()), []),
  {ok, Data4} = wait_for_log_stream(raft_log:stream(NewLog4, self()), []),
  ?assertEqual([{3, "test_3_1"}, {2, "test_2"}], Data3),
  ?assertEqual([], Data4).

retention_time(_Config) ->
  Log = raft_log:new([{retention_time, 1}]),
  LogTen = lists:foldl(fun raft_log:append/2, Log, lists:seq(1, 100)),

  % wait the retention to kick in
  timer:sleep(2005),

  {log_ref, Pid, _LogPos} = Log,
  SendAllRef = {log_ref, Pid, undefined},

  {ok, Data1} = wait_for_log_stream(raft_log:stream(SendAllRef, self()), []),
  ?assertEqual([], Data1),

  NewLog2 = raft_log:append("test_1", LogTen),
  {ok, Data2} = wait_for_log_stream(raft_log:stream(SendAllRef, self()), []),
  ?assertEqual([{101, "test_1"}], Data2),

  {ok, Data3} = wait_for_log_stream(raft_log:stream(NewLog2, self()), []),
  ?assertEqual([], Data3),

  timer:sleep(2005),
  _NewLog3 = raft_log:append("test_1", NewLog2),

  % fallback for first item
  {ok, _}  = wait_for_log_stream(raft_log:stream(Log, self()), []),

  {ok, Data4} = wait_for_log_stream(raft_log:stream(SendAllRef, self()), []),
  ?assertEqual([{102, "test_1"}], Data4).


retention_time_not_enough_token(_Config) ->
  Log = raft_log:new([{retention_time, 1}]),
  _LogTen = lists:foldl(fun raft_log:append/2, Log, lists:seq(1, 250)),
  % wait the retention to kick in
  timer:sleep(2005),

  {log_ref, Pid, _LogPos} = Log,
  SendAllRef = {log_ref, Pid, undefined},
  {ok, Data1} = wait_for_log_stream(raft_log:stream(SendAllRef, self()), []),
  ?assertEqual(lists:reverse([{I, I} || I <- lists:seq(101, 250)]), Data1),

  timer:sleep(1005),
  {ok, Data2} = wait_for_log_stream(raft_log:stream(SendAllRef, self()), []),
  ?assertEqual(lists:reverse([{I, I} || I <- lists:seq(201, 250)]), Data2).

wait_for_log_stream({error, Error}, _Acc) ->
  {error, Error};
wait_for_log_stream({ok, StreamRef}, Acc) ->
  wait_for_log_stream(StreamRef, Acc);
wait_for_log_stream(StreamRef, Acc) ->
  case raft_log:read_stream(StreamRef, 1200) of
    {ok, Pos, Data} ->
      wait_for_log_stream(StreamRef, [{Pos, Data} | Acc]);
    '$end_of_stream' ->
      {ok, Acc};
    timeout ->
      timeout
  end.
