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
  [ last_entries_new_log,
    append_log,
    append_log_overwrite,
    log_not_found,
    next_index,
    delete,
    delete_unknown,
    destroy,
    append_commands
  ].

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------
last_entries_new_log(_Config) ->
  LogRef = raft_log:new(),
  ?assertEqual(0, raft_log:last_index(LogRef)),
  ?assertEqual(0, raft_log:last_term(LogRef)).

append_log(_Config) ->
  LogRef = raft_log:new(),
  LogRef2 = raft_log:append(LogRef, test_command, 5),
  ?assertEqual(1, raft_log:last_index(LogRef2)),
  ?assertEqual(5, raft_log:last_term(LogRef2)).

append_log_overwrite(_Config) ->
  LogRef = raft_log:new(),
  _LogRef2 = raft_log:append(LogRef, test_command, 5),
  LogRef3 = raft_log:append(LogRef, test_command2, 6),
  ?assertEqual(1, raft_log:last_index(LogRef3)),
  ?assertEqual(6, raft_log:last_term(LogRef3)),
  ?assertEqual({ok, {6, test_command2}}, raft_log:get(LogRef3, 1)).

log_not_found(_Config) ->
  LogRef = raft_log:new(),
  ?assertEqual(not_found, raft_log:get(LogRef, 100)).

next_index(_Config) ->
  LogRef = raft_log:new(),
  ?assertEqual(1, raft_log:next_index(LogRef)),
  LogRef2 = raft_log:append(LogRef, test_command, 1),
  ?assertEqual(2, raft_log:next_index(LogRef2)).

delete(_Config) ->
  LogRef = raft_log:new(),
  Entries = 10,
  LogRef2 = lists:foldl(fun(I, LRef) ->
                          raft_log:append(LRef, {test, I}, 1)
                        end, LogRef, lists:seq(1, Entries)),

  Index = 5,
  ?assertEqual({ok, {1, {test, Index}}}, raft_log:get(LogRef2, Index)),
  LogRef3 = raft_log:delete(LogRef2, 5),

  ?assertEqual(not_found, raft_log:get(LogRef3, Index)),
  ?assertEqual(not_found, raft_log:get(LogRef3, Index+1)),
  ?assertEqual({ok, {1, {test, Index-1}}},
               raft_log:get(LogRef3, Index-1)).

delete_unknown(_Config) ->
  LogRef = raft_log:new(),
  LogRef2 = raft_log:delete(LogRef, 10),
  ?assertEqual(0, raft_log:last_index(LogRef2)).

destroy(_Config) ->
  LogRef = raft_log:new(),
  ?assertEqual(ok, raft_log:destroy(LogRef)).

append_commands(_Config) ->
  LogRef = raft_log:new(),
  Command = fun (I) -> {store, test, I} end,
  NewLogRef = raft_log:append_commands(LogRef, [{1, Command(1)},
                                                {1, Command(2)}], 1),
  ?assertEqual(2, raft_log:last_index(NewLogRef)),
  ?assertEqual(1, raft_log:last_term(NewLogRef)),

  % do not append command when already set
  NewLogRef2 = raft_log:append_commands(NewLogRef, [{1, Command(1)},
                                                    {1, Command(2)},
                                                    {2, Command(3)}],
                                        1),
  ?assertEqual(3, raft_log:last_index(NewLogRef2)),
  ?assertEqual(2, raft_log:last_term(NewLogRef2)),
  ?assertEqual({ok, 3, [{1, Command(1)},
                        {1, Command(2)},
                        {2, Command(3)}]}, raft_log:list(NewLogRef2, 1, 100)),

  NewLogRef3 = raft_log:append_commands(NewLogRef2, [{2, Command(4)}], 4),

  ?assertEqual(4, raft_log:last_index(NewLogRef3)),
  ?assertEqual(2, raft_log:last_term(NewLogRef3)),
  ?assertEqual({ok, 4, [{1, Command(1)},
                        {1, Command(2)},
                        {2, Command(3)},
                        {2, Command(4)}]}, raft_log:list(NewLogRef3, 1, 100)),
  ?assertEqual({ok, 3, [{1, Command(1)},
                        {1, Command(2)},
                        {2, Command(3)}]}, raft_log:list(NewLogRef3, 1, 2)).
