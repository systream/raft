-module(prop_raft_log).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-record(state, {
  log_ref :: raft_log:log_ref(),
  req_ids = [] :: [binary()]
}).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                application:ensure_all_started(raft),
                {History, State, Result} = run_commands(?MODULE, Cmds),
                raft_log:destroy(State#state.log_ref),
                ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                    [History,State,Result]),
                          aggregate(command_names(Cmds), Result =:= ok))
            end).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
    #state{log_ref = raft_log:new()}.

%% @doc List of possible commands to run against the system
command(#state{log_ref = LogRef}) ->
    frequency([
        {10, {call, raft_log, append, [LogRef, req_id(), command(), raft_term()]}},
        {5, {call, raft_log, store_snapshot, [LogRef, index(), raft_term(), user_state()]}},
        {4, {call, raft_log, delete, [LogRef, index()]}},
        {6, {call, raft_log, is_logged, [LogRef, req_id()]}},
        {4, {call, raft_log, get_term, [LogRef, index(), raft_term()]}},
        {3, {call, raft_log, get, [LogRef, index()]}}
    ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_LogRef, {call, _Mod, _Fun, _Args}) ->
    true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{log_ref = LogRefState},
              {call, raft_log, append, [_LogRef, ReqId, Cmd, RaftTerm]}, NewLogRef) ->
  LastIndex = raft_log:last_index(NewLogRef),
  {ok, IndexTerm} = raft_log:get_term(NewLogRef, LastIndex),
  raft_log:last_term(NewLogRef) =:= RaftTerm andalso
  RaftTerm =:= IndexTerm andalso
  LastIndex > raft_log:last_index(LogRefState) andalso
  raft_log:get(NewLogRef, LastIndex) =:= {ok, {RaftTerm, ReqId, Cmd}} andalso
  raft_log:is_logged(NewLogRef, ReqId) =:= true andalso
  raft_log:next_index(NewLogRef) =:= LastIndex+1;
postcondition(_State,
              {call, raft_log, store_snapshot, [_LogRef, Index, RaftTerm, UserState]},
              NewLogRef) ->
  {snapshot, {LastIncludedTerm, LastIncludedIndex, SnUserState}} = raft_log:get(NewLogRef, Index),
  {ok, GTTerm} = raft_log:get_term(NewLogRef, Index),
  LastIncludedTerm =:= RaftTerm andalso
  GTTerm =:= LastIncludedTerm andalso
  GTTerm =:= RaftTerm andalso
  LastIncludedIndex =:= Index andalso
  SnUserState =:= UserState;
postcondition(_State,
              {call, raft_log, delete, [_LogRef, Index]},
              NewLogRef) ->
  not_found =:= raft_log:get(NewLogRef, Index) andalso
  no_term =:= raft_log:get_term(NewLogRef, Index) andalso
  Index >= raft_log:next_index(NewLogRef);
postcondition(#state{req_ids = ReqIds},
              {call, raft_log, is_logged, [_LogRef, ReqId]},
              true) ->
  lists:member(ReqId, ReqIds);
postcondition(#state{req_ids = _ReqIds},
              {call, raft_log, is_logged, [_LogRef, _ReqId]},
              false) ->
  % with bloom false positives may happen
  true;
postcondition(#state{}, {call, raft_log, get_term, [_LogRef, _Index, _Def]}, _Result) ->
  true;
postcondition(#state{}, {call, raft_log, get, [_LogRef, _Index]}, _Result) ->
  true.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#state{req_ids = ReqIds} = State, NewLogRef,
           {call, raft_log, append, [_, ReqId, _Cmd, _Term]}) ->
  State#state{log_ref = NewLogRef, req_ids = [ReqId | ReqIds]};
next_state(State, NewLogRef, {call, raft_log, store_snapshot, _}) ->
  State#state{log_ref = NewLogRef};
next_state(State, NewLogRef, {call, raft_log, delete, _}) ->
  State#state{log_ref = NewLogRef};
next_state(State, _Result, {call, _Module, _Fun, _Args}) ->
  State.

req_id() ->
  ?LET(S, integer(), begin <<"req_id", (erlang:integer_to_binary(S))/binary>> end).

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
  map(binary(), oneof([map(binary(), binary()), binary()])).