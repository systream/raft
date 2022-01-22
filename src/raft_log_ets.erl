%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2022, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_log_ets).
-author("Peter Tihanyi").

-include("raft.hrl").

-behavior(raft_log).

%% API
-export([new/1, store/3, lookup/2, delete/2, destroy/1]).

-spec new(binary() | undefined) -> State when State :: term().
new(_ServerId) ->
  {0, ets:new(raft_log, [public, ordered_set, {keypos, 1}])}.

-spec store(State, log_index(), raft_log:log_entry()) -> State when State :: term().
store(Ref, Index, Data) ->
  ets:insert(Ref, {Index, Data}),
  Ref.

-spec lookup(term(), log_index()) -> {ok, raft_log:log_entry()} | not_found.
lookup(Ref, Index) ->
  case ets:lookup(Ref, Index) of
    [] ->
      not_found;
    [{_, Data}] ->
      {ok, Data}
  end.

-spec delete(State, log_index()) -> State when State :: term().
delete(Ref, Index) ->
  ets:delete(Ref, Index),
  Ref.

-spec destroy(State :: term()) -> ok.
destroy(Ref) ->
  ets:delete(Ref),
  ok.