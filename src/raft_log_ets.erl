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
-export([new/0, append/2, lookup/2, delete/2, destroy/1]).

-spec new() -> State when State :: term().
new() ->
  ets:new(raft_log, [public, ordered_set, {keypos, 2}]).

-spec append(State, raft_log:log_entry()) -> State when State :: term().
append(Ref, Data) ->
  ets:insert(Ref, Data),
  Ref.

-spec lookup(State, log_index()) -> State when State :: term().
lookup(Ref, Index) ->
  case ets:lookup(Ref, Index) of
    [] ->
      not_found;
    [Data] ->
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