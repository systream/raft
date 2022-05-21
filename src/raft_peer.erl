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
  next_index = 1 :: log_index(),
  match_index = 0 :: log_index() | 0
}).

-type(peer() :: #raft_peer{}).

-export_type([peer/0]).

%% API
-export([new/2,
         send/2,
         match_index/1, match_index/2,
         next_index/1, server/1, next_index/2]).

-spec new(pid(), log_index()) -> peer().
new(Server, NextIndex) when is_pid(Server) ->
  #raft_peer{server = Server,
             next_index = NextIndex,
             match_index = 0}.

-spec send(peer(), Msg :: term()) -> peer().
send(#raft_peer{server = Server} = Peer, Msg) ->
  erlang:send_nosuspend(Server, Msg),
  Peer.

-spec next_index(peer()) -> log_index().
next_index(#raft_peer{next_index = NextIndex}) ->
  NextIndex.

-spec next_index(peer(), log_index()) -> peer().
next_index(Peer, NextIndex) ->
  Peer#raft_peer{next_index = NextIndex}.

-spec server(peer()) -> pid().
server(#raft_peer{server = Server}) ->
  Server.

-spec match_index(peer()) -> log_index().
match_index(#raft_peer{match_index = PeerMatchIndex}) ->
  PeerMatchIndex.

-spec match_index(peer(), log_index()) -> peer().
match_index(#raft_peer{match_index = PeerMatchIndex} = Peer, MatchIndex)
  when MatchIndex > PeerMatchIndex ->
  Peer#raft_peer{match_index = MatchIndex};
match_index(Peer, _MatchIndex) ->
  Peer.
