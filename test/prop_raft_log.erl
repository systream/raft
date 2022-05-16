-module(prop_raft_log).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").


%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-record(state, {
  log_ref :: raft_log:log_ref()
}).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
              begin
                  application:ensure_all_started(raft),
                  {History, State, Result} = run_commands(?MODULE, Cmds),
                  raft_log:destroy(State#state.log_ref),
                  ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                      [History,State,Result]),
                            aggregate(command_names(Cmds), Result =:= ok))
              end)
            ).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
    #state{log_ref = raft_log:new()}.

%% @doc List of possible commands to run against the system
command(#state{log_ref = LogRef}) ->
  Commands = [
      {10, {call, raft_log, append, [LogRef, command(), raft_term()]}}
    , {5, {call, raft_log, store_snapshot, [LogRef, index(), user_state()]}}
    , {4, {call, raft_log, delete, [LogRef, index()]}}
    , {4, {call, raft_log, get_term, [LogRef, index(), raft_term()]}}
    , {7, {call, raft_log, get, [LogRef, index()]}}
      %, {2, {call, raft_log, list, [LogRef, index(), pos_integer()]}}
  ],
  frequency(Commands).


%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_LogRef, {call, _Mod, _Fun, _Args} = _A) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{log_ref = LogRefState},
              {call, raft_log, append, [_LogRef, Cmd, RaftTerm]}, NewLogRef) ->
  LastIndex = raft_log:last_index(NewLogRef),
  {ok, IndexTerm} = raft_log:get_term(NewLogRef, LastIndex),
  raft_log:last_term(NewLogRef) =:= RaftTerm andalso
  RaftTerm =:= IndexTerm andalso
  LastIndex > raft_log:last_index(LogRefState) andalso
  raft_log:get(NewLogRef, LastIndex) =:= {ok, {RaftTerm, Cmd}} andalso
  raft_log:next_index(NewLogRef) =:= LastIndex+1;
postcondition(_State,
              {call, raft_log, store_snapshot, [_LogRef, _Index, _UserState]},
              {error, index_not_found}) ->
  true;
postcondition(_State,
              {call, raft_log, store_snapshot, [_LogRef, Index, UserState]},
              {ok, NewLogRef}) ->
  {snapshot, {LastIncludedTerm, LastIncludedIndex, SnUserState}} =
    raft_log:get(NewLogRef, Index),
  {ok, GTTerm} = raft_log:get_term(NewLogRef, Index),
  %LastIncludedTerm =:= RaftTerm andalso
  GTTerm =:= LastIncludedTerm andalso
  %GTTerm =:= RaftTerm andalso
  LastIncludedIndex =:= Index andalso
  SnUserState =:= UserState;
postcondition(_State,
              {call, raft_log, delete, [_LogRef, Index]},
              NewLogRef) ->
  not_found =:= raft_log:get(NewLogRef, Index) andalso
  no_term =:= raft_log:get_term(NewLogRef, Index) andalso
  Index >= raft_log:next_index(NewLogRef);
postcondition(#state{}, {call, raft_log, get_term, [_LogRef, _Index, _Def]}, _Result) ->
  true;
postcondition(#state{}, {call, raft_log, get, [_LogRef, _Index]}, _Result) ->
  true;
postcondition(#state{}, {call, raft_log, list, [_LogRef, _Index, MaxChunk]},
              {ok, _EndIndex, List}) ->
  length(List) =< MaxChunk;
postcondition(#state{}, {call, raft_log, list, [_LogRef, _Index, _MaxChunk]},
              {snapshot, {_LastTerm, _LastIndex, _UserState}}) ->
  true.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#state{} = State, NewLogRef,
           {call, raft_log, append, [_, _Cmd, _Term]}) ->
  State#state{log_ref = NewLogRef};
next_state(State, {ok, NewLogRef}, {call, raft_log, store_snapshot, _}) ->
  State#state{log_ref = NewLogRef};
next_state(State, NewLogRef, {call, raft_log, delete, _}) ->
  State#state{log_ref = NewLogRef};
next_state(State, _Result, {call, _Module, _Fun, _Args}) ->
  State.

command() ->
  oneof([
          {store, binary(), map(binary(), binary())},
          {do_stuff, binary()}
        ]).

raft_term() ->
  pos_integer().

index() ->
  pos_integer().

user_state() ->
  resize(1, map(binary(), oneof([map(binary(), binary()), binary()]))).