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

-define(BLOOM_CHUNK_SIZE, 100000).

-export([new/0, new/1,
         append/4, append_commands/3,
         destroy/1,
         last_index/1, last_term/1,
         get/2, delete/2, get_term/3, list/3,
         next_index/1, get_term/2, store_snapshot/3, is_logged/2]).

-record(log_ref, {data_ref :: term(),
                  bloom_ref :: reference(),
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
  req_id :: binary(),
  command :: command()
}).

-type(log_entry() :: #log_entry{}).

-record(snapshot_entry, {
  last_included_index :: log_index(),
  last_included_term :: raft_term(),
  bloom :: binary(),
  state :: term()
}).

-type(snapshot_entry() :: #snapshot_entry{}).

-export_type([log_ref/0, log_entry/0, snapshot_entry/0]).

-callback new() -> State when State :: term().
-callback store(State, log_index(), log_entry()) -> State when State :: term().
-callback lookup(State :: term(), log_index()) -> {ok, raft_log:log_entry()} | not_found.
-callback delete(State, log_index()) -> State when State :: term().
-callback destroy(State :: term()) -> ok.

-spec new() -> log_ref().
new() ->
  new(raft_log_ets).

-spec new(module()) -> log_ref().
new(Callback) ->
  #log_ref{data_ref = apply(Callback, new, []), callback = Callback, bloom_ref = new_bloom(0)}.

-spec is_logged(log_ref(), req_id()) -> boolean().
is_logged(#log_ref{bloom_ref = BloomRef} = Ref, ReqId) ->
  case ebloom:contains(BloomRef, ReqId) of
    true ->
      is_req_logged(Ref, Ref#log_ref.last_index, ReqId);
    false ->
      false
  end.

-spec is_req_logged(log_ref(), log_index(), req_id()) -> boolean().
is_req_logged(Ref, Index, ReqId) ->
  case get(Ref, Index) of
    {ok, {_Term, ReqId, _Command}} ->
      true;
    {ok, _} ->
      is_req_logged(Ref, Index-1, ReqId);
    not_found ->
      false
  end.



-spec last_index(log_ref()) -> log_index().
last_index(#log_ref{last_index = Pos}) ->
  Pos.

-spec last_term(log_ref()) -> raft_term().
last_term(#log_ref{last_term = Term}) ->
  Term.

-spec append(log_ref(), binary(), command(), raft_term()) -> log_ref().
append(#log_ref{data_ref = Ref, last_term = LastTerm,
                callback = Cb, bloom_ref = BloomRef} = LogRef, ReqId, Command, Term) ->
  NewPos = next_index(LogRef),
  logger:debug("Storing log entry on index ~p with term ~p command ~p", [NewPos, Term, Command]),
  LogEntry = #log_entry{log_index = NewPos, req_id = ReqId, term = Term, command = Command},
  NewRef = apply(Cb, store, [Ref, NewPos, LogEntry]),
  ok = ebloom:insert(BloomRef, ReqId),
  LogRef#log_ref{last_index = NewPos,
                 last_term = Term,
                 bloom_ref = maybe_upgrade_bloom(NewPos, BloomRef),
                 penultimate_term = LastTerm,
                 data_ref = NewRef}.

-spec append_commands(log_ref(), [{raft_term(), req_id(), command()}], log_index()) -> log_ref().
append_commands(LogRef, [], _NextIndex) ->
  LogRef;
append_commands(#log_ref{last_index = LastIndex} = Log,
                [{Term, ReqId, Command} | RestCommands], Index) ->
  case LastIndex >= Index of
    true ->
      append_commands(Log, RestCommands, Index+1);
    false ->
      append_commands(append(Log, ReqId, Command, Term), RestCommands, Index+1)
  end.

-spec store_snapshot(log_ref(), log_index(), term()) ->
  log_ref().
store_snapshot(#log_ref{last_snapshot_index = LastSnapshotIndex} = Ref, Index, _UserState)
  when LastSnapshotIndex >= Index andalso LastSnapshotIndex =/= undefined ->
  Ref;
store_snapshot(#log_ref{data_ref = DataRef, callback = Cb, bloom_ref = _BloomRef,
                        last_snapshot_index = LastSnapshotIndex} = Ref,
               Index, UserState) ->
  {ok, LastSnapshotTerm} = get_term(Ref, Index),
  Snapshot = #snapshot_entry{
    last_included_index = Index,
    last_included_term = LastSnapshotTerm,
    %bloom = ebloom:serialize(BloomRef),
    state = UserState
  },
  logger:warning("Storing snapshot for index ~p", [Index]),
  NewRef = apply(Cb, store, [DataRef, Index, Snapshot]),
  % cleanup
  From = case LastSnapshotIndex of
           undefined -> 1;
           _ -> LastSnapshotIndex
         end,
  To = Index-1,
  logger:warning("Cleanup indexes from ~p to ~p ", [From, To]),
  NewRef2 = lists:foldl(fun(IndexToDel, R) ->
                apply(Cb, delete, [R, IndexToDel])
              end, NewRef, lists:seq(From, To)),
  Ref#log_ref{data_ref = NewRef2, last_snapshot_index = Index}.

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
  case raft_log:get(Log, Index) of
    {ok, {LTerm, _ReqId, _Command}} ->
      {ok, LTerm};
    not_found ->
      no_term
  end.

-spec get(log_ref(), log_index()) -> {ok, {raft_term(), req_id(), command()}} | not_found.
get(#log_ref{data_ref = Ref, callback = Callback}, Index) ->
  case apply(Callback, lookup, [Ref, Index]) of
    {ok, #log_entry{term = Term, req_id = ReqId, command = Command}} ->
      {ok, {Term, ReqId, Command}};
    not_found ->
      not_found
  end.

-spec list(log_ref(), log_index(), pos_integer()) ->
  {ok, log_index(), list({term(), req_id(), command()})}.
list(#log_ref{last_index = LastIndex} = Log, FromIndex, MaxChunk) when FromIndex =< LastIndex ->
  EndIndex = min(FromIndex+MaxChunk, LastIndex),
  {ok, EndIndex, get_list(Log, EndIndex, FromIndex, [])};
list(#log_ref{last_index = LastIndex}, _FromIndex, _MaxChunk) ->
  {ok, LastIndex, []}.

-spec get_list(log_ref(), log_index(), log_index(), list({term(), command()})) ->
  list({term(), req_id(), command()}).
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

-spec new_bloom(log_index()) -> reference().
new_bloom(Index) ->
  BloomChunkSize = (Index div ?BLOOM_CHUNK_SIZE) + ?BLOOM_CHUNK_SIZE,
  {ok, BloomRef} = ebloom:new(BloomChunkSize, 0.001, rand:uniform(1000)),
  BloomRef.

-spec maybe_upgrade_bloom(log_index(), reference()) -> reference().
maybe_upgrade_bloom(Index, BloomRef) when Index rem ?BLOOM_CHUNK_SIZE =:= 0 ->
  NewBloom = new_bloom(Index),
  logger:warning("Upgrading bloom filter at index ~p", [Index]),
  ebloom:union(NewBloom, BloomRef),
  NewBloom;
maybe_upgrade_bloom(_Index, BloomRef) ->
  BloomRef.