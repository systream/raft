%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft).
-author("Peter Tihanyi").

-export([start/1, stop/1,
         join/2, leave/2,
         status/1,
         command/2, query/2]).

-spec start(module()) -> {ok, pid()} | {error, term()}.
start(Callback) ->
  raft_server_sup:start_server(Callback).

-spec stop(pid()) -> ok.
stop(Server) ->
  raft_server:stop(Server).

-spec command(pid(), term()) ->
  term().
command(ClusterMember, Command) ->
  raft_server:command(ClusterMember, Command).

-spec query(pid(), term()) ->
  term().
query(ClusterMember, Command) ->
  raft_server:query(ClusterMember, Command).

-spec join(pid(), pid()) -> ok | {error, no_leader | leader_changed}.
join(ActualClusterMember, NewClusterMember) ->
  raft_server:join(ActualClusterMember, NewClusterMember).

-spec leave(pid(), pid()) -> ok | {error, no_leader | leader_changed}.
leave(ClusterMember, MemberToLeave) ->
  raft_server:leave(ClusterMember, MemberToLeave).

-spec status(pid()) -> map().
status(ClusterMember) ->
  raft_server:status(ClusterMember).
