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
         append/3, append_commands/3,
         destroy/1,
         last_index/1, last_term/1,
         get/2, delete/2, get_term/3, list/3,
         next_index/1]).

-record(log_ref, {data_ref :: ets:tid(),
                  last_index = ?INITIAL_INDEX :: log_index() | 0,
                  last_term = ?INITIAL_TERM :: raft_term(),
                  penultimate_term :: raft_term() | undefined
                  }).

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
append(#log_ref{data_ref = Ref, last_term = LastTerm} = LogRef, Command, Term) ->
  NewPos = next_index(LogRef),
  logger:debug("Storing log entry on index ~p with term ~p command ~p", [NewPos, Term, Command]),
  ets:insert(Ref, #log_entry{log_index = NewPos, term = Term, command = Command}),
  LogRef#log_ref{last_index = NewPos, last_term = Term, penultimate_term = LastTerm}.

-spec append_commands(log_ref(), [{raft_term(), command()}], log_index()) -> log_ref().
append_commands(LogRef, [], _NextIndex) ->
  LogRef;
append_commands(#log_ref{last_index = LastIndex} = Log, [{Term, Command} | RestCommands], Index) ->
  case LastIndex > Index of
    true ->
      append_commands(Log, RestCommands, Index+1);
    false ->
      append_commands(append(Log, Command, Term), RestCommands, Index+1)
  end.

-spec get_term(log_ref(), log_index() | 0, raft_term()) -> raft_term().
get_term(#log_ref{last_index = LastIndex, penultimate_term = Term}, Index, _DefaultTerm) when
  Term =/= undefined andalso LastIndex-1 =:= Index  ->
  Term;
get_term(#log_ref{}, 0, DefaultTerm) ->
  DefaultTerm;
get_term(#log_ref{last_index = Index, last_term = Term}, Index, _DefaultTerm) ->
  Term;
get_term(Log, Index, DefaultTerm) ->
  case raft_log:get(Log, Index) of
    {ok, {LTerm, _Command}} ->
      LTerm;
    not_found ->
      DefaultTerm
  end.

-spec get(log_ref(), log_index()) -> {ok, {raft_term(), command()}} | not_found.
get(#log_ref{data_ref = Ref}, Index) ->
  case ets:lookup(Ref, Index) of
      [] ->
          not_found;
      [#log_entry{term = Term, command = Command}] ->
          {ok, {Term, Command}}
  end.

-spec list(log_ref(), log_index(), pos_integer()) ->
  list({term(), command()}).
list(#log_ref{last_index = LastIndex} = Log, FromIndex, MaxChunk) ->
  EndIndex = min(FromIndex + MaxChunk, LastIndex),
  get_list(Log, EndIndex, FromIndex, []).

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

-spec delete(log_ref(), log_index()) -> log_ref().
delete(#log_ref{data_ref = Ref, last_index = LastIndex} = LogRef, Index) when LastIndex >= Index ->
    ets:delete(Ref, LastIndex),
    delete(LogRef#log_ref{last_index = LastIndex-1}, Index);
delete(#log_ref{last_index = LastIndex} = LogRef, _Index) ->
    case get(LogRef, LastIndex) of
        {ok, {LastTerm, _LastCommand}} ->
            LogRef#log_ref{last_term = LastTerm, penultimate_term = undefined};
        not_found when LastIndex =:= ?INITIAL_INDEX ->
            LogRef#log_ref{last_term = ?INITIAL_TERM, penultimate_term = undefined}
    end.

-spec destroy(log_ref()) -> ok.
destroy(#log_ref{data_ref = Ref}) ->
  ets:delete(Ref),
  ok.
