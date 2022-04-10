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
  peer :: pid(),
  next_index = 1 :: log_index(),
  match_index = 0 :: log_index() | 0
}).

-type(peer() :: #raft_peer{}).

-export_type([peer/0]).

%% API
-export([new/2, replicated/2, next_index/1, server/1, next_index/2]).

-spec new(pid(), raft_log:log_ref()) -> peer().
new(Server, Log) when is_pid(Server) ->
  #raft_peer{peer = Server,
             next_index = raft_log:last_index(Log),
             match_index = 0}.

-spec next_index(peer()) -> log_index().
next_index(#raft_peer{next_index = NextIndex}) ->
  NextIndex.

-spec next_index(peer(), log_index()) -> peer().
next_index(Peer, NextIndex) ->
  Peer#raft_peer{next_index = NextIndex}.

-spec server(peer()) -> pid().
server(#raft_peer{peer = Server}) ->
  Server.

-spec replicated(peer(), log_index()) -> peer().
replicated(#raft_peer{match_index = PeerMatchIndex} = Peer, MatchIndex)
  when MatchIndex > PeerMatchIndex ->
  Peer#raft_peer{match_index = MatchIndex};
replicated(Peer, _MatchIndex) ->
  Peer.
  %maybe_update_next_index(Peer, MatchIndex).

%-spec maybe_update_next_index(peer(), log_index()) -> peer().
%maybe_update_next_index(#raft_peer{next_index = NextIndex} = Peer, Index) when Index >=NextIndex ->
%  Peer#raft_peer{next_index = Index+1};
%maybe_update_next_index(Peer, _Index) ->
%  Peer.