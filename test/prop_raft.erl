-module(prop_raft).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3,
         stop_collaborator/2, kill_collaborator/1, new_member/0, join_member/2]).

-record(state, {
  collaborators = [] :: [pid()],
  storage = #{}
}).

kill_collaborator(Pid) ->
  exit(Pid, kill),
  ok.

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
  application:ensure_all_started(raft),
  setup_config(),
  application:set_env(raft, max_heartbeat_timeout, 1000),
  application:set_env(raft, min_heartbeat_timeout, 100),
  application:set_env(raft, heartbeat_grace_time, 50),
  put(raft_s, start_cluster()),
  ?FORALL(Cmds, commands(?MODULE),
          ?TRAPEXIT(
              begin
                {History, State, Result} = run_commands(?MODULE, Cmds),
                ?WHENFAIL(
                  begin
                    io:format("History: ~p\nState: ~p\nResult: ~p\n", [History,State,Result])
                  end,
                  aggregate(command_names(Cmds), Result =:= ok)
                )
              end)
          ).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
  initial_state([]).

initial_state(Collaborators) ->
  %io:format(user, "new State: ~p~n", [Collaborators]),
  On = hd(get(raft_s)),
  raft:command(On, clear_state),
  #state{collaborators = Collaborators}.

%% @doc List of possible commands to run against the system
command(_State) ->
  On = oneof(get(raft_s)),
  frequency([
    {10, {call, raft, command, [On, {store, store_key(), pos_integer()}]}},
    {5, {call, raft, query, [On, {get, store_key()}]}}%,
    %{3, {call, ?MODULE, join_member, [On, new_member()]}}
   % {3, {call, raft, leave, [On, oneof(State#test_state.collaborators)]}},
   % {1, {call, ?MODULE, kill_collaborator, [oneof(State#test_state.collaborators)]}},
    %{1, {call, ?MODULE, stop_collaborator, [On, ?SUCHTHAT(S, oneof(get(raft_s)), S =/= On)]}}
 ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(#state{}, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{} = _State,
              {call, raft, command, [_On, {store, _Key, _Value}]}, ok) ->
  true;
postcondition(#state{} = _State,
              {call, raft, command, [_On, {store, _Key, _Value}]}, {error, no_leader}) ->
  true;
postcondition(#state{} = _State,
              {call, raft, command, [_On, {store, _Key, _Value}]}, Res) ->
  io:format(user, "cmd res: ~p~n", [Res]),
  false;
postcondition(#state{} = _State, {call, raft, query, [_On, {get, _Key}]}, {error, no_leader}) ->
  true;
postcondition(#state{storage = Storage, collaborators = Collabs} = _State,
              {call, raft, query, [On, {get, Key}]}, Value) ->
  %io:format(user, "get on ~p Key: ~p -> Value: ~p~n Storage: ~p~n", [On, Key, Value, Storage]),
  lists:all(fun(Pid) ->
              QValue = raft:query(Pid, {get, Key}),
              case Value =:= QValue of
                false ->
                  io:format(user, "Get on ~p ~p => ~p~n", [On, Key, Value]),
                  io:format(user, "~p Qvalue: ~p~n", [Pid, QValue]),
                  io:format(user, "sys: ~p~n stat: ~p~n",
                            [sys:get_state(Pid), sys:get_status(Pid)]);
                true ->
                  ok
              end,
              Value =:= QValue
            end, Collabs -- [On]) andalso
  maps:get(Key, Storage, no_value) =:= Value;
postcondition(_State, {call, ?MODULE, join_member, [_On, _NewMember]}, _Res) ->
  true;
postcondition(_State, {call, ?MODULE, stop_collaborator, [_On, _MemberToLeave]}, _Res) ->
  true;
postcondition(_State, {call, _Mod, _Fun, _Args} = A, _Res) ->
  io:format("unhandled post condition: ~p~n~p~n", [A, _Res]),
  false.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#state{storage = Storage} = State, ok,
           {call, raft, command, [_On, {store, Key, Value}]}) ->
  %io:format(user, "store: ~p => ~p ===> ~p~n", [Key, Value, Storage#{Key => Value}]),
  State#state{storage = Storage#{Key => Value}};
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
  %io:format(user, "no nextstat: ~p~n", [State]),
  State.

store_key() ->
  integer(1, 100).

setup_config() ->
  Level = warning,
  Config = #{ config => #{drop_mode_qlen => 1000000000,
                          file => "log/raft.log",
                          burst_limit_enable => false,
                          flush_qlen => 1000000000,
                          max_no_bytes => 10485760,
                          max_no_files => 5,
                          sync_mode_qlen => 100},
              formatter => {logger_formatter,
                            #{max_size => 200000,
                              template => [time, " " , level, " [", pid, ",", raft_role, "] ", msg, "\n"]
                            }
              },
              level => Level
           },
  logger:add_handler(raft, logger_std_h, Config),
  logger:set_primary_config(level, Level),
  Meta = #{raft_role => proper_tester},
  NewMeta = case logger:get_process_metadata() of
              undefined ->
                Meta;
              OldMeta ->
                maps:merge(OldMeta, Meta)
            end,
  logger:set_process_metadata(NewMeta).

until(Fun) ->
  until(Fun, 500).

until(Fun, MaxRetry) ->
  case Fun() of
    {error, no_leader} when MaxRetry>0 ->
      timer:sleep(10),
      until(Fun, MaxRetry-1);
    {error, leader_changed} when MaxRetry>0 ->
      until(Fun, MaxRetry-1);
    {error, already_member} ->
      ok;
    Else ->
      Else
  end.

start_cluster() ->
  PidA = new_member(),
  PidB = new_member(),
  PidC = new_member(),
  ok = until(fun() -> raft:join(PidA, PidB) end),
  ok = until(fun() -> raft:join(PidA, PidC) end),
  [PidA, PidB, PidC].

new_member() ->
  {ok, Pid} = raft:start(raft_test_cb),
  %io:format(user, "new member: ~p~n", [Pid]),
  Pid.

join_member(On, Member) ->
  ok = until(fun() -> raft:join(On, Member) end),
  Members = get(raft_s),
  put(raft_s, [Member | Members]),
  ok.

stop_collaborator(On, Pid) ->
  ok = until(fun() -> raft:leave(On, Pid) end),
  Members = get(raft_s),
  put(raft_s, lists:delete(Pid, Members)),
  spawn(fun() -> raft:stop(Pid), timer:sleep(10000), exit(Pid, kill) end).