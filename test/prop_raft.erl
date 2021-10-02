-module(prop_raft).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3,
         stop_collaborator/1, start_collaborator/0, kill_collaborator/1, status/1]).

-record(state, {
  collaborators = [] :: [pid()],
  in_cluster = [] :: [pid()],
  storage = #{}
}).

start_collaborator() ->
  {ok, Pid} = raft:start(raft_test_cb),
  Pid.

stop_collaborator(Pid) ->
  exit(Pid, normal),
  ok.

kill_collaborator(Pid) ->
  exit(Pid, kill),
  ok.

status([]) ->
  ok;
status([Pid | Tail]) ->
  case raft:status(Pid) of
    {Type, Term, Leader, Collaborators} ->
      io:format(user, "[~p - ~p[~p]] Leader: ~p~nCollaborators: ~p~n",
                [Pid, Type, Term, Leader, Collaborators]);
    _ ->
      io:format(user, "[~p] procs died~n", [Pid])
  end,
  status(Tail).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
    application:ensure_all_started(raft),
    application:set_env(raft, max_heartbeat_timeout, 2500),
    application:set_env(raft, min_heartbeat_timeout, 500),
    application:set_env(raft, heartbeat_grace_time, 1000),
    application:set_env(raft, consensus_timeout, 1000),
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
  CollaboratorCount = 3,
  Collaborators = [start_collaborator() || _ <- lists:seq(1, CollaboratorCount)],
  [Leader | Followers] = Collaborators,
  [raft:join(Leader, Follower) || Follower <- Followers],
  #state{collaborators = Collaborators, in_cluster = Collaborators}.

%% @doc List of possible commands to run against the system
command(#state{collaborators = Collaborators} = _State) ->
    OnCollaborator = oneof(Collaborators),
    frequency([
      {50, {call, raft, command, [OnCollaborator, {store, store_key(), pos_integer()}]}},
      {8, {call, raft, join, [OnCollaborator, oneof(Collaborators)]}},
      {2, {call, raft, leave, [OnCollaborator, oneof(Collaborators)]}},
      {6, {call, ?MODULE, start_collaborator, []}},
      %{1, {call, ?MODULE, kill_collaborator, [oneof(Collaborators)]}},
      %{1, {call, ?MODULE, stop_collaborator, [oneof(Collaborators)]}},
      {5, {call, ?MODULE, status, [Collaborators]}}
    ]).
%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_State, {call, _Mod, _Fun, _Args}) ->
  io:format(user, "Now running: ~p~n", [{_Mod, _Fun, _Args}]),
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{storage = _Storage} = _State,
              {call, raft, command, [_On, {store, _Key, _Value}]}, {error, _}) ->
  true;
postcondition(#state{storage = Storage, in_cluster = InCluster} = _State,
              {call, raft, command, [On, {store, Key, Value}]}, StoreValue) ->
  StorageGet = maps:get(Key, Storage, 0),
  io:format(user, "Get ~p increase ~p => ~p (~p)~n", [Key, Value, StoreValue, StorageGet]),
  case lists:member(On, InCluster) of
    true ->
      case raft:command(On, {get, Key}) of
        GetValue when GetValue >= (StorageGet+Value) ->
          true;
        Else ->
          io:format(user, "Value should be ~p of key \"~p\" instead of ~p on node ~p~n ~p ->  ~p~n",
                    [Value, Key, Else, On, raft:status(On), _State]),
          false
      end;
    _ ->
      true
  end;
postcondition(_State, {call, raft, join, [_On, _Target]}, ok) ->
  true;
postcondition(_State, {call, raft, leave, [_On, _Target]}, ok) ->
  true;
postcondition(_State, {call, ?MODULE, start_collaborator, []}, _Pid) ->
  true;
postcondition(_State, {call, ?MODULE, kill_collaborator, [_Pid]}, ok) ->
  true;
postcondition(_State, {call, ?MODULE, stop_collaborator, [_Pid]}, ok) ->
  true;
postcondition(_State, {call, ?MODULE, status, [_]}, ok) ->
  true;
postcondition(_State, {call, _Mod, _Fun, _Args} = A, _Res) ->
    io:format("unhandled post condition: ~p~n~p~n", [A, _Res]),
    false.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#state{} = State, {error, _Error},
           {call, ?MODULE, command, [_On, {store, _Key, _Value}]}) ->
  State;
next_state(#state{storage = Storage, in_cluster = InCluster} = State, Value,
           {call, ?MODULE, command, [On, {store, Key, _Value}]}) ->
  case lists:member(On, InCluster) of
    true ->
      State#state{storage = Storage#{Key => Value}};
    _ ->
      State
  end;

next_state(#state{in_cluster = InCluster} = State, ok,
           {call, raft, join, [On, Target]}) ->
  case lists:member(On, InCluster) of
    true ->
      State#state{in_cluster = [Target | InCluster]};
    _ ->
      State
  end;

next_state(#state{in_cluster = InCluster} = State, ok,
           {call, raft, leave, [On, Target]}) ->
  case {lists:member(On, InCluster), lists:member(Target, InCluster)} of
    {true, true} ->
      State#state{in_cluster = lists:delete(Target, [InCluster])};
    _ ->
      State
  end;

next_state(#state{collaborators = Collaborators} = State, Pid,
           {call, ?MODULE, start_collaborator, []}) ->
  State#state{collaborators = [Pid | Collaborators]};

next_state(#state{collaborators = Collaborators, in_cluster = InCluster} = State, ok,
           {call, ?MODULE, kill_collaborator, [Pid]}) ->
  State#state{collaborators = lists:delete(Pid, Collaborators),
              in_cluster = lists:delete(Pid, InCluster)};
next_state(#state{collaborators = Collaborators, in_cluster = InCluster} = State, ok,
           {call, ?MODULE, stop_collaborator, [Pid]}) ->
  State#state{collaborators = lists:delete(Pid, Collaborators),
              in_cluster = lists:delete(Pid, InCluster)};
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    State.

store_key() ->
  integer(1, 100).