%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2022, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_peer).
-author("Peter Tihanyi").

-include("raft.hrl").

-record(raft_peer, {
    server :: pid(),
    %included_in_consensus = true :: boolean(),
    log :: raft_log:log_ref(),
    next_index :: log_index(),
    match_index :: log_index() | 0
  %in_flight_msg
}).

-type(raft_peer() :: #raft_peer{}).

%@TODO not copy
-record(append_entries_req, {
    term :: raft_term(),
    leader :: pid(),
    prev_log_index :: log_index(),
    prev_log_term :: raft_term(),
    entries :: [command()],
    leader_commit_index :: log_index()
}).

%% API
-export([new/2, match_index/2, has_more_to_replicate/1,
         decrease_match_index/1,
         send_append_req/2, send_append_reqs/2]).


new(Server, Log) when is_pid(Server) ->
    #raft_peer{server = Server,
               next_index = raft_log:next_index(Log),
               match_index = raft_log:last_index(Log)};
new(Cluster, Log) ->
  Peers = raft_cluster:members(Cluster)--[raft_cluster:leader(Cluster)],
  lists:foldl(fun(Server, Acc) -> Acc#{Server => new(Server, Log)} end, #{}, Peers).

decrease_match_index(#raft_peer{match_index = 0} = Peer) ->
    Peer;
decrease_match_index(#raft_peer{match_index = MatchIndex} = Peer) ->
    Peer#raft_peer{match_index = MatchIndex-1}.

match_index(Peer, MatchIndex) ->
    Peer#raft_peer{match_index = MatchIndex}.

has_more_to_replicate(#raft_peer{match_index = MatchIndex, next_index = NextIndex})
    when MatchIndex+1 < NextIndex-1  ->
    true;
has_more_to_replicate(_) ->
    false.

send_append_reqs(Peers, Log) ->
  maps:map(fun(_, Peer) -> raft_peer:send_append_req(Peer, Log) end, Peers).

send_append_req(#raft_peer{match_index = MatchIndex, server = Server}, Log) ->
    LastTermTerm = get_last_term(Log, MatchIndex),
    {ok, {Term, Commands}} = get_log(Log, MatchIndex+1),
    logger:debug("Send append log entry to ~p", [Server]),
    erlang:send_nosuspend(Server, #append_entries_req{term = Term,
                                                        leader = self(),
                                                        prev_log_index = MatchIndex,
                                                        prev_log_term = LastTermTerm,
                                                        entries = Commands,
                                                        leader_commit_index = raft_log:last_index(Log)}).



get_last_term(Log, Index) ->
    case raft_log:last_index(Log) =:= Index of
        true ->
            raft_log:last_term(Log);
        false ->
            case raft_log:get(Log, Index) of
               {ok, {LTerm, _Command}} ->
                   LTerm;
               not_found ->
                   raft_log:last_term(Log)
           end
    end.

get_log(Log, Index) ->
    case raft_log:get(Log, Index) of
        {ok, {Term, Command}} ->
            {ok, {Term, [Command]}};
        not_found ->
            {ok, {raft_log:last_term(Log), []}}
    end.

