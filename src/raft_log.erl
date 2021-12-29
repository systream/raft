%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(raft_log).

-include("raft.hrl").

-export([new/0,
         append/3,
         destroy/1,
         last_index/1, last_term/1, get/2, delete/2]).

-record(log_ref, {data_ref :: ets:tid(),
                  last_index = 0 :: log_index(),
                  last_term = 0 :: raft_term(),
                  prev_index = 0 :: log_index(),
                  prev_term = 0 :: raft_term()}).
-type(log_ref() :: #log_ref{}).

-export_type([log_ref/0]).

-record(log_entry, {
    log_index :: log_index(),
    term :: raft_term(),
    command :: command()
}).

-spec new() -> log_ref().
new() ->
  #log_ref{data_ref = ets:new(raft_log, [public, ordered_set])}.

-spec last_index(log_ref()) -> log_index().
last_index(#log_ref{last_index = Pos}) ->
  Pos.

-spec last_term(log_ref()) -> raft_term().
last_term(#log_ref{last_term = Term}) ->
  Term.

-spec append(log_ref(), command(), raft_term()) -> log_ref().
append(#log_ref{data_ref = Ref, last_index = Pos} = LogRef, Command, Term) ->
  NewPos = Pos+1,
  ets:insert(Ref, #log_entry{log_index = NewPos, term = Term, command = Command}),
  LogRef#log_ref{last_index = NewPos, last_term = Term}.

-spec get(log_ref(), log_index()) -> {ok, {log_index(), raft_term(), command()}} | not_found.
get(#log_ref{data_ref = Ref}, Index) ->
  case ets:lookup(Ref, Index) of
      [] ->
          not_found;
      [#log_entry{log_index = Index, term = Term, command = Command}] ->
          {ok, {Index, Term, Command}}
  end.

-spec delete(log_ref(), log_index()) -> log_ref().
delete(#log_ref{data_ref = Ref, last_index = LastIndex} = LogRef, Index) when LastIndex < Index ->
    ets:delete(Ref, LastIndex),
    delete(LogRef#log_ref{last_index = LastIndex-1}, Index);
delete(LogRef, Index) ->
    {ok, {_Index, Term, _Command}} = get(LogRef, Index),
    LogRef#log_ref{last_term = Term}.

-spec destroy(log_ref()) -> ok | {error, term()}.
destroy(#log_ref{data_ref = Ref}) ->
  ets:delete(Ref).
