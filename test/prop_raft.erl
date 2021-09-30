-module(prop_raft).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3,
         stop_collaborator/1, start_collaborator/0, kill_collaborator/1, status/1]).

-record(state, {
  collaborators = [] :: [pid()],
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
    CollaboratorCount = 3,
    Collaborators = [start_collaborator() || _ <- lists:seq(1, CollaboratorCount)],
    [Leader | Followers] = Collaborators,
    [raft:join(Leader, Follower) || Follower <- Followers],
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
    Collaborators = [Pid || {_Id, Pid, _Type, _Modules} <- supervisor:which_children(raft_sup)],
    #state{collaborators = Collaborators}.

%% @doc List of possible commands to run against the system
command(#state{collaborators = Collaborators} = _State) ->
    OnCollaborator = oneof(Collaborators),
    frequency([
                  {50, {call, raft, command, [OnCollaborator, {store, term(), pos_integer()}]}},
                  {10, {call, raft, join, [OnCollaborator, oneof(Collaborators)]}},
                  {8, {call, raft, leave, [OnCollaborator, oneof(Collaborators)]}},
                  {6, {call, ?MODULE, start_collaborator, []}},
                  %{5, {call, ?MODULE, kill_collaborator, [oneof(Collaborators)]}},
                  %{5, {call, ?MODULE, stop_collaborator, [oneof(Collaborators)]}},
                  {5, {call, ?MODULE, status, [Collaborators]}}
    ]).
%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_State, {call, _Mod, _Fun, _Args}) ->
    true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{storage = Storage} = _State,
              {call, raft, command, [On, {store, Key, Value}]}, StoreValue) ->
  StorageGet = maps:get(Key, Storage, 0),
  io:format(user, "Get ~p increase ~p => ~p (~p)~n", [Key, Value, StoreValue, StorageGet]),
  case raft:command(On, {get, Key}) of
    GetValue when GetValue >= (StorageGet+Value) ->
      true;
    Else ->
      io:format(user, "Value should be ~p of ~p instead of ~p on node ~p~n",
                [Value, Key, Else, On]),
      false
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
next_state(#state{storage = Storage} = State, Value,
           {call, ?MODULE, command, [_On, {store, Key, _Value}]}) ->
  State#state{storage = Storage#{Key => Value}};
next_state(#state{collaborators = Collaborators} = State, Pid,
           {call, ?MODULE, start_collaborator, []}) ->
  State#state{collaborators = [Pid | Collaborators]};
next_state(#state{collaborators = Collaborators} = State, ok,
           {call, ?MODULE, kill_collaborator, [Pid]}) ->
  State#state{collaborators = lists:delete(Pid, Collaborators)};
next_state(#state{collaborators = Collaborators} = State, ok,
           {call, ?MODULE, stop_collaborator, [Pid]}) ->
  State#state{collaborators = lists:delete(Pid, Collaborators)};
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    State.