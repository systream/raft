-module(prop_raft).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3,
         stop_collaborator/1, add_collaborator/1, kill_collaborator/1]).

-record(state, {
  collaborators = [] :: [pid()],
  storage = #{}
}).

add_collaborator(ClusterMember) ->
  {ok, Pid} = raft:start(raft_test_cb),
  timer:sleep(125), % @TODO handle!!!
  case catch raft:join(ClusterMember, Pid) of
    ok ->
      {ok, Pid};
    Else ->
      Else
  end.

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
  logger:set_primary_config(level, debug),
  ?FORALL(Cmds, parallel_commands(?MODULE),
          begin
              {History, State, Result} = run_parallel_commands(?MODULE, Cmds),
              ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                  [History,State,Result]),
                        aggregate(command_names(Cmds), Result =:= ok))
          end).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
  {ok, Pid} = raft:start(raft_test_cb),
  Collaborators = [Pid],
  #state{collaborators = Collaborators}.

%% @doc List of possible commands to run against the system
command(#state{collaborators = Collaborators} = _State) ->
  OnCollaborator = oneof(Collaborators),
  frequency([
    {50, {call, raft, command, [OnCollaborator, {store, store_key(), pos_integer()}]}},
    {8, {call, ?MODULE, add_collaborator, [OnCollaborator]}},
    {2, {call, raft, leave, [OnCollaborator, oneof(Collaborators)]}},
    {1, {call, ?MODULE, kill_collaborator, [oneof(Collaborators)]}},
    {1, {call, ?MODULE, stop_collaborator, [oneof(Collaborators)]}}
  ]).
%% @doc Determines whether a command should be valid under the
%% current state.
precondition(#state{collaborators = [_Item]}, {call, raft, leave, [_]}) ->
  false;
precondition(#state{collaborators = [_Item]}, {call, ?MODULE, kill_collaborator, [_]}) ->
  false;
precondition(#state{collaborators = [_Item]}, {call, ?MODULE, stop_collaborator, [_]}) ->
  false;
precondition(_State, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{storage = Storage, collaborators = Cluster} = _State,
              {call, raft, command, [On, {store, Key, Value}]}, {ok, _StoredValue}) ->
  StorageGet = maps:get(Key, Storage, 0),
  case lists:member(On, Cluster) of
    true ->
      case raft:command(On, {get, Key}) of
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
postcondition(_State, {call, ?MODULE, add_collaborator, [_On]}, {ok, _NewMember}) ->
  true;
postcondition(_State, {call, raft, leave, [_On, _Target]}, ok) ->
  true;
postcondition(_State, {call, ?MODULE, kill_collaborator, [_Pid]}, ok) ->
  true;
postcondition(_State, {call, ?MODULE, stop_collaborator, [_Pid]}, ok) ->
  true;
postcondition(_State, {call, _Mod, _Fun, _Args} = A, _Res) ->
  io:format("unhandled post condition: ~p~n~p~n", [A, _Res]),
  false.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#state{storage = Storage, collaborators = InCluster} = State, {ok, Value},
           {call, ?MODULE, command, [On, {store, Key, _Value}]}) ->
  case lists:member(On, InCluster) of
    true ->
      State#state{storage = Storage#{Key => Value}};
    _ ->
      State
  end;

next_state(#state{collaborators = InCluster} = State, {ok, NewMember},
           {call, ?MODULE, add_collaborator, [_On]}) ->
  State#state{collaborators = [NewMember | InCluster]};

next_state(#state{collaborators = InCluster} = State, ok,
           {call, raft, leave, [_On, Target]}) ->
  State#state{collaborators = lists:delete(Target, InCluster)};

next_state(#state{collaborators = InCluster} = State, ok,
           {call, ?MODULE, kill_collaborator, [Target]}) ->
  State#state{collaborators = lists:delete(Target, InCluster)};

next_state(#state{collaborators = InCluster} = State, ok,
           {call, ?MODULE, stop_collaborator, [Target]}) ->
  State#state{collaborators = lists:delete(Target, InCluster)};

next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    State.

store_key() ->
  integer(1, 100).