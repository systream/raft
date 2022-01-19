-module(prop_raft).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3,
         stop_collaborator/1, kill_collaborator/1]).

-record(test_state, {
  collaborators = [] :: [pid()],
  storage = #{}
}).

stop_collaborator(Pid) ->
  raft:stop(Pid).

kill_collaborator(Pid) ->
  exit(Pid, kill),
  ok.

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
  application:ensure_all_started(raft),
  setup_config(),
  ?FORALL(Cmds, commands(?MODULE),
          begin
            {History, State, Result} = run_commands(?MODULE, Cmds),
            ?WHENFAIL(
              begin
                io:format("History: ~p\nState: ~p\nResult: ~p\n", [History,State,Result])
              end,
              aggregate(command_names(Cmds), Result =:= ok)
            )
          end).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
  {ok, Pid} = raft:start(raft_test_cb),
  #test_state{collaborators = [Pid]}.

%% @doc List of possible commands to run against the system
command(State) ->
  On = oneof(State#test_state.collaborators),
  frequency([
    {10, {call, raft, command, [On, {store, store_key(), pos_integer()}]}}
    %{4, {call, raft, join, [On, frequency([{10, new_member()}, {1, oneof(State#test_state.collaborators)}])]}},
   % {3, {call, raft, leave, [On, oneof(State#test_state.collaborators)]}},
   % {1, {call, ?MODULE, kill_collaborator, [oneof(State#test_state.collaborators)]}},
   % {1, {call, ?MODULE, stop_collaborator, [oneof(State#test_state.collaborators)]}}
 ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(#test_state{}, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#test_state{storage = Storage, collaborators = Cluster} = _State,
              {call, raft, command, [On, {store, Key, Value}]}, {ok, _StoredValue}) ->
  StorageGet = maps:get(Key, Storage, 0),
  case lists:member(On, Cluster) of
    true ->
      case raft:query(On, {get, Key}) of
        GetValue when GetValue >= (StorageGet+Value) ->
          true;
        Else ->
          io:format(user, "Value should be ~p of key \"~p\" instead of ~p on node ~p~n ~p -> ~p~n",
                    [Value, Key, Else, On, raft:status(On), _State]),
          false
      end;
    _ ->
      true
  end;
postcondition(#test_state{} = _State, {call, raft, command, _}, {error, no_leader}) ->
  true;
postcondition(_State, {call, _Mod, _Fun, _Args} = A, _Res) ->
  io:format("unhandled post condition: ~p~n~p~n", [A, _Res]),
  false.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#test_state{storage = Storage, collaborators = InCluster} = State, {ok, Value},
           {call, raft, command, [On, {store, Key, _Value}]}) ->
  case lists:member(On, InCluster) of
    true ->
      State#test_state{storage = Storage#{Key => Value}};
    _ ->
      State
  end;
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
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
