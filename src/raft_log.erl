%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(raft_log).

-include("raft.hrl").

-define(INITIAL_TERM, 0).
-define(INITIAL_INDEX, 0).

-export([new/0, new/2,
         append/3, append_commands/3,
         destroy/1,
         last_index/1, last_term/1,
         get_term/3, get_term/2,
         get/2,
         delete/2,
         list/3,
         next_index/1,
         store_snapshot/3, store_snapshot/4]).

-record(log_ref, {data_ref :: term(),
                  callback :: module(),
                  last_index = ?INITIAL_INDEX :: log_index() | 0,
                  last_term = ?INITIAL_TERM :: raft_term(),
                  penultimate_term :: raft_term() | undefined,
                  last_snapshot_index :: log_index() | undefined
                  }).

-type(log_ref() :: #log_ref{}).

-record(log_entry, {
  log_index :: log_index(),
  term :: raft_term(),
  command :: command()
}).

-type(log_entry() :: #log_entry{}).

-record(snapshot_entry, {
  last_included_index :: log_index(),
  last_included_term :: raft_term(),
  state :: term()
}).

-type(snapshot_entry() :: #snapshot_entry{}).

-export_type([log_ref/0, log_entry/0, snapshot_entry/0]).

-callback new(ServerId :: binary() | undefined) -> State when State :: term().
-callback store(State, log_index(), log_entry()) -> State when State :: term().
-callback lookup(State :: term(), log_index()) -> {ok, raft_log:log_entry()} | not_found.
-callback delete(State, log_index()) -> State when State :: term().
-callback destroy(State :: term()) -> ok.

-spec new() -> log_ref().
new() ->
  new(undefined, raft_log_ets).

-spec new(binary() | undefined, module()) -> log_ref().
new(ServerId, Callback) ->
  {LastIndex, Ref} = apply(Callback, new, [ServerId]),
  LastTerm = case LastIndex of
            0 ->
              ?INITIAL_TERM;
            _ ->
              case apply(Callback, lookup, [Ref, LastIndex]) of
                {ok, #log_entry{term = Term}} -> Term;
                {ok, #snapshot_entry{last_included_term = Term}} -> Term
              end
          end,
  #log_ref{data_ref = Ref,
           last_index = LastIndex,
           last_term = LastTerm,
           callback = Callback}.

-spec last_index(log_ref()) -> log_index() | 0.
last_index(#log_ref{last_index = Pos}) ->
  Pos.

-spec last_term(log_ref()) -> raft_term().
last_term(#log_ref{last_term = Term}) ->
  Term.

-spec append(log_ref(), command(), raft_term()) -> log_ref().
append(#log_ref{data_ref = Ref, last_term = LastTerm,
                callback = Cb} = LogRef, Command, Term) ->
  NewPos = next_index(LogRef),
  logger:debug("Storing log entry on index ~p with term ~p command ~p", [NewPos, Term, Command]),
  LogEntry = #log_entry{log_index = NewPos, term = Term, command = Command},
  NewRef = apply(Cb, store, [Ref, NewPos, LogEntry]),
  LogRef#log_ref{last_index = NewPos,
                 last_term = Term,
                 penultimate_term = LastTerm,
                 data_ref = NewRef}.

-spec append_commands(log_ref(), [{raft_term(), command()}], log_index()) -> log_ref().
append_commands(LogRef, [], _NextIndex) ->
  LogRef;
append_commands(#log_ref{last_index = LastIndex} = Log,
                [{Term, Command} | RestCommands], Index) ->
  case LastIndex >= Index of
    true ->
      append_commands(Log, RestCommands, Index+1);
    false ->
      append_commands(append(Log, Command, Term), RestCommands, Index+1)
  end.

-spec store_snapshot(log_ref(), log_index(), raft_term(), term()) ->
  log_ref().
store_snapshot(#log_ref{data_ref = DataRef, callback = Cb,
                        last_snapshot_index = LastSnapshotIndex} = Ref,
               Index, LastSnapshotTerm, UserState) ->
  Snapshot = #snapshot_entry{
    last_included_index = Index,
    last_included_term = LastSnapshotTerm,
    state = UserState
  },
  logger:info("Storing snapshot for index ~p", [Index]),
  NewDataRef = apply(Cb, store, [DataRef, Index, Snapshot]),
  % cleanup
  NewRef = maybe_cleanup(Ref#log_ref{data_ref = NewDataRef}, LastSnapshotIndex, Index),
  case NewRef#log_ref.last_index =< Index of
    true ->
      NewRef#log_ref{last_snapshot_index = Index,
                     last_index = Index, last_term = LastSnapshotTerm,
                     penultimate_term = undefined};
    _ when LastSnapshotIndex =< Index orelse LastSnapshotIndex =:= undefined ->
      NewRef#log_ref{last_snapshot_index = Index,
                     penultimate_term = undefined};
    _ ->
      NewRef#log_ref{penultimate_term = undefined}
  end.

-spec store_snapshot(log_ref(), log_index(), term()) ->
  {ok, log_ref()} | {error, index_not_found}.
store_snapshot(#log_ref{last_snapshot_index = LastSnapshotIndex} = Ref, Index, _UserState)
  when LastSnapshotIndex >= Index andalso LastSnapshotIndex =/= undefined ->
  {ok, Ref};
store_snapshot(Ref, Index, UserState) ->
  case get_term(Ref, Index) of
    {ok, LastSnapshotTerm} ->
      {ok, store_snapshot(Ref, Index, LastSnapshotTerm, UserState)};
    no_term ->
      {error, index_not_found}
  end.

-spec get_term(log_ref(), log_index() | 0, term()) -> raft_term().
get_term(LogRef, Index, DefaultTerm) ->
  case get_term(LogRef, Index) of
    {ok, Term} ->
      Term;
    no_term ->
      DefaultTerm
  end.

-spec get_term(log_ref(), log_index() | 0) -> {ok, raft_term()} | no_term.
get_term(#log_ref{last_index = LastIndex, penultimate_term = Term}, Index) when
  Term =/= undefined andalso LastIndex-1 =:= Index  ->
  {ok, Term};
get_term(#log_ref{}, 0) ->
  no_term;
get_term(#log_ref{last_index = Index, last_term = Term}, Index) ->
  {ok, Term};
get_term(Log, Index) ->
  case get_by_callback(Log, Index) of
    {ok, #log_entry{term = Term}} ->
      {ok, Term};
    {ok, #snapshot_entry{last_included_term = LastIncludedTerm}} ->
      {ok, LastIncludedTerm};
    not_found ->
      no_term
  end.

-spec get(log_ref(), log_index()) ->
  {ok, {raft_term(), command()}} |
  {snapshot, {raft_term(), log_index(), term()}} | not_found.
get(Ref, Index) ->
  case get_by_callback(Ref, Index) of
    {ok, #log_entry{term = Term, command = Command}} ->
      {ok, {Term, Command}};
    {ok, #snapshot_entry{last_included_index = LastIncludedIndex,
                         last_included_term = LastIncludedTerm,
                         state = UserState}} ->
      {snapshot, {LastIncludedTerm, LastIncludedIndex, UserState}};
    not_found ->
      not_found
  end.

-spec get_by_callback(log_ref(), log_index()) -> {ok, log_entry() | snapshot_entry()} | not_found.
get_by_callback(#log_ref{callback = Callback, data_ref = Ref}, Index) ->
  apply(Callback, lookup, [Ref, Index]).

-spec list(log_ref(), log_index(), pos_integer()) ->
  {ok, log_index(), list({term(), command()})} |
  {snapshot, {raft_term(), log_index(), term()}}.
list(#log_ref{last_snapshot_index = SnapshotIndex} = Log, FromIndex, _MaxChunk)
  when FromIndex =< SnapshotIndex andalso SnapshotIndex =/= undefined ->
  {snapshot, _} = get(Log, SnapshotIndex);
list(#log_ref{last_index = LastIndex} = Log, FromIndex, MaxChunk) when FromIndex =< LastIndex ->
  EndIndex = min(FromIndex+MaxChunk, LastIndex),
  {ok, EndIndex, get_list(Log, EndIndex, FromIndex, [])};
list(#log_ref{last_index = LastIndex}, _FromIndex, _MaxChunk) ->
  {ok, LastIndex, []}.

-spec get_list(log_ref(), log_index(), log_index(), list({term(), command()})) ->
  list({term(), command()}).
get_list(Log, CurrentIndex, FromIndex, Acc) when CurrentIndex >= FromIndex ->
  {ok, Command} = get(Log, CurrentIndex),
  get_list(Log, CurrentIndex-1, FromIndex, [Command | Acc]);
get_list(_Log, _CurrentIndex, _FromIndex, Acc) ->
  Acc.

-spec next_index(log_ref()) -> log_index().
next_index(#log_ref{last_index = LastIndex}) ->
  LastIndex+1.

-spec delete(log_ref(), log_index() | 0) -> log_ref().
delete(#log_ref{data_ref = Ref, last_index = LastIndex, callback = Callback} = LogRef, Index)
  when LastIndex >= Index ->
    NewRef = apply(Callback, delete, [Ref, LastIndex]),
    delete(LogRef#log_ref{last_index = LastIndex-1, data_ref = NewRef}, Index);
delete(#log_ref{last_snapshot_index = SnapShotIndex} = LogRef, Index)
  when SnapShotIndex > Index andalso SnapShotIndex =/= undefined ->
  delete(LogRef#log_ref{last_snapshot_index = undefined}, Index);
delete(#log_ref{last_index = LastIndex} = LogRef, _Index) ->
  case get_term(LogRef, LastIndex) of
    {ok, LastTerm} ->
      LogRef#log_ref{last_term = LastTerm, penultimate_term = undefined};
    no_term when LastIndex =:= ?INITIAL_INDEX ->
      LogRef#log_ref{last_term = ?INITIAL_TERM, penultimate_term = undefined}
  end.

-spec destroy(log_ref()) -> ok.
destroy(#log_ref{data_ref = Ref, callback = Callback}) ->
  apply(Callback, destroy, [Ref]).

maybe_cleanup(Ref, undefined, Index) ->
  maybe_cleanup(Ref, 1, Index);
maybe_cleanup(Ref = #log_ref{data_ref = DataRef, callback = Cb}, From, Index)
  when From =< Index-1 ->
  To = Index-1,
  logger:debug("Cleanup indexes from ~p to ~p ", [From, To]),
  NewDataRef = lists:foldl(fun(IndexToDel, CDataRef) ->
                             apply(Cb, delete, [CDataRef, IndexToDel])
                           end, DataRef, lists:seq(From, To)),
  Ref#log_ref{data_ref = NewDataRef};
maybe_cleanup(Ref, _, _) ->
  Ref.