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
         command/2,

         test_join/0]).

test_join() ->
  logger:set_application_level(raft, debug),
  logger:set_primary_config(level, debug),
  {ok, A} = start(raft_test_cb),
  {ok, B} = start(raft_test_cb),
  {ok, C} = start(raft_test_cb),
  {ok, D} = start(raft_test_cb),
  {ok, E} = start(raft_test_cb),
  timer:sleep(100),
  io:format(user, "========= started ===========~n~n", []),
  raft:join(A, B),
  raft:join(A, E),
  raft:join(A, D),

  raft:command(A, {store,  test, 1}),
  raft:command(B, {store,  test, 1}),
  raft:join(A, C),
  raft:join(A, D),
  raft:join(A, E),
  print(status(A)),
  print(status(B)),
  print(status(C)),
  print(status(D)),
  print(status(E)),
  [A, B, C, D, E].

print(Status) ->
  io:format(user, "~p~n", [Status]).


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

-spec join(pid(), pid()) -> ok.
join(ActualClusterMember, NewClusterMember) ->
  raft_server:join(ActualClusterMember, NewClusterMember).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  raft_server:leave(ClusterMember, MemberToLeave).

-spec status(pid()) -> map().
status(ClusterMember) ->
  raft_server:status(ClusterMember).