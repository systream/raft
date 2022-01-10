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

-export([new/0,
         append/3,
         destroy/1,
         last_index/1, last_term/1, get/2, delete/2, next_index/1]).

-record(log_ref, {data_ref :: ets:tid(),
                  last_index = ?INITIAL_INDEX :: log_index() | 0,
                  last_term = ?INITIAL_TERM :: raft_term()}).

-type(log_ref() :: #log_ref{}).

-export_type([log_ref/0]).

-record(log_entry, {
    log_index :: log_index(),
    term :: raft_term(),
    command :: command()
}).

-spec new() -> log_ref().
new() ->
  #log_ref{data_ref = ets:new(raft_log, [public, ordered_set, {keypos, 2}])}.

-spec last_index(log_ref()) -> log_index().
last_index(#log_ref{last_index = Pos}) ->
  Pos.

-spec last_term(log_ref()) -> raft_term().
last_term(#log_ref{last_term = Term}) ->
  Term.

-spec append(log_ref(), command(), raft_term()) -> log_ref().
append(#log_ref{data_ref = Ref} = LogRef, Command, Term) ->
  NewPos = next_index(LogRef),
  logger:debug("Storing log entry on index ~p with term ~p command ~p", [NewPos, Term, Command]),
  ets:insert(Ref, #log_entry{log_index = NewPos, term = Term, command = Command}),
  LogRef#log_ref{last_index = NewPos, last_term = Term}.

-spec get(log_ref(), log_index()) -> {ok, {raft_term(), command()}} | not_found.
get(#log_ref{data_ref = Ref}, Index) ->
  case ets:lookup(Ref, Index) of
      [] ->
          not_found;
      [#log_entry{log_index = Index, term = Term, command = Command}] ->
          {ok, {Term, Command}}
  end.

-spec next_index(log_ref()) -> log_index().
next_index(#log_ref{last_index = LastIndex}) ->
    LastIndex+1.

-spec delete(log_ref(), log_index()) -> log_ref().
delete(#log_ref{data_ref = Ref, last_index = LastIndex} = LogRef, Index) when LastIndex >= Index ->
    ets:delete(Ref, LastIndex),
    delete(LogRef#log_ref{last_index = LastIndex-1}, Index);
delete(#log_ref{last_index = LastIndex} = LogRef, _Index) ->
    case get(LogRef, LastIndex) of
        {ok, {LastTerm, _LastCommand}} ->
            LogRef#log_ref{last_term = LastTerm};
        not_found when LastIndex =:= ?INITIAL_INDEX ->
            LogRef#log_ref{last_term = ?INITIAL_TERM}
    end.

-spec destroy(log_ref()) -> ok.
destroy(#log_ref{data_ref = Ref}) ->
  ets:delete(Ref),
  ok.
