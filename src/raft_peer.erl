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
  %log :: raft_log:log_ref(),
  next_index = 1 :: log_index(),
  match_index = 0 :: log_index() | 0
  %in_flight_requests
}).

-type(peer() :: #raft_peer{}).

-export_type([peer/0]).

%% API
-export([new/2, replicated/2,
         decrease_next_index/1, next_index/1, server/1, next_index/2]).

new(Server, Log) when is_pid(Server) ->
    #raft_peer{server = Server,
               next_index = raft_log:next_index(Log),
               match_index = 0}.

next_index(#raft_peer{next_index = NextIndex}) ->
  NextIndex.

next_index(Peer, NextIndex) ->
  Peer#raft_peer{next_index = NextIndex}.

server(#raft_peer{server = Server}) ->
  Server.

decrease_next_index(#raft_peer{next_index = 1} = Peer) ->
  Peer;
decrease_next_index(#raft_peer{next_index = 2} = Peer) ->
  Peer#raft_peer{next_index = 1};
decrease_next_index(#raft_peer{next_index = NextIndex} = Peer) ->
  Peer#raft_peer{next_index = NextIndex-2}.

replicated(#raft_peer{match_index = PeerMatchIndex} = Peer, MatchIndex)
  when MatchIndex > PeerMatchIndex ->
  maybe_update_next_index(Peer#raft_peer{match_index = MatchIndex}, MatchIndex);
replicated(Peer, MatchIndex) ->
  maybe_update_next_index(Peer, MatchIndex).

maybe_update_next_index(#raft_peer{next_index = NextIndex} = Peer, Index) when Index >= NextIndex ->
  Peer#raft_peer{next_index = Index+1};
maybe_update_next_index(Peer, _Index) ->
  Peer.


