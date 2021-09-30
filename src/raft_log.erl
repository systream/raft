%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(raft_log).

-record(log_ref, {pid :: pid(),
                  last_pos = 0 :: non_neg_integer()}).

-type(log_ref() :: #log_ref{}).

-export_type([log_ref/0]).

-export([new/0, new/1,
         append/2,
         destroy/1,
         stream/2, read_stream/2, get_pos/1]).

-spec new() -> log_ref().
new() ->
  new([]).

-spec new(Opt :: proplists:proplist()) -> log_ref().
new(Opts) ->
  {ok, Pid} = raft_log_sup:start_worker(Opts),
  #log_ref{pid = Pid}.

-spec get_pos(log_ref()) -> non_neg_integer() | undefined.
get_pos(#log_ref{last_pos = Pos}) ->
  Pos.

-spec append(term(), log_ref()) -> log_ref().
append(Command, #log_ref{pid = Pid, last_pos = Pos} = Ref) ->
  {ok, NewPos} = raft_log_worker:append(Pid, Pos, Command),
  Ref#log_ref{last_pos = NewPos}.

-spec stream(log_ref(), pid()) -> {ok, term()} | {error, term()}.
stream(#log_ref{pid = Pid, last_pos = undefined}, TargetPid) ->
  raft_log_worker:stream(Pid, TargetPid, first);
stream(#log_ref{pid = Pid, last_pos = Pos}, TargetPid) ->
  raft_log_worker:stream(Pid, TargetPid, Pos).

-spec read_stream(term(), non_neg_integer() | infinity) ->
  {ok, non_neg_integer(), term()} | '$end_of_stream' | timeout.
read_stream(StreamRef, Timeout) ->
  raft_log_worker:read_stream(StreamRef, Timeout).

-spec destroy(log_ref()) -> ok | {error, term()}.
destroy(#log_ref{pid = Pid}) ->
  raft_log_sup:stop_worker(Pid).
