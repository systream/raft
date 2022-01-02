%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft).
-author("Peter Tihanyi").

-export([start/1, join/2, leave/2, status/1, command/2, test/0, test_mult/0, test_join/0]).


test_mult() ->
  {Ok, []} = rpc:multicall(raft, start, [raft_process_registry]),
  [{ok, FirstPid} | Rest] = Ok,
  [ok = raft:join(FirstPid, Pid)  || {ok, Pid} <- Rest],
  FirstPid.

test_join() ->

  application:set_env(raft, max_heartbeat_timeout, 12500),
  application:set_env(raft, min_heartbeat_timeout, 11500),
  application:set_env(raft, heartbeat_grace_time, 500),
  application:set_env(raft, consensus_timeout, 13000),
  {ok, A} = start(raft_test_cb),
  {ok, B} = start(raft_test_cb),
  timer:sleep(300),
  io:format(user, "========= started ===========~n~n", []),
  raft:join(A, B),
  print(status(A)),
  print(status(B)).

test() ->
  application:set_env(raft, max_heartbeat_timeout, 15000),
  application:set_env(raft, min_heartbeat_timeout, 5000),
  application:set_env(raft, heartbeat_grace_time, 10000),
  application:set_env(raft, consensus_timeout, 3000),
  {ok, A} = start(raft_test_cb),
  {ok, B} = start(raft_test_cb),
  {ok, C} = start(raft_test_cb),
  {ok, D} = start(raft_test_cb),
  {ok, E} = start(raft_test_cb),
  join(C, D),
  Parent = self(),
  Pids = [spawn(fun() ->
                  [command(A, {store, {I, X}, I}) || I <- lists:seq(1, 5)],
                  Parent ! {ready, self()}
                end) || X <- lists:seq(1, 3)],
  [receive {ready, Pid} -> ok end || Pid <- Pids],
  timer:sleep(1000),
  join(B, A),
  join(A, C),
  join(A, D),
  join(A, E),
  timer:sleep(5000),
  print(status(A)),
  [print(status(Collaborator)) || Collaborator <- [A, B, C, D, E]],

  command(A, {store, {1, 2}, 3}),

  timer:sleep(5000),
  print(status(A)),
  [print(status(Collaborator)) || Collaborator <- [A, B, C, D, E]],

  timer:sleep(5000),
  print(status(A)),
  [print(status(Collaborator)) || Collaborator <- [A, B, C, D, E]],

  timer:sleep(5000),
  print(status(A)),
  [print(status(Collaborator)) || Collaborator <- [A, B, C, D, E]],
  [A, B, C, D, E].

print({Type, Term, Leader, Collaborators}) ->
  io:format(user, "[~p,~p (~p)] -> ~p~n", [Leader, Type, Term, Collaborators]).


-spec start(module()) -> {ok, pid()} | {error, term()}.
start(Callback) ->
  raft_server_sup:start_server(Callback).

-spec command(pid(), term()) ->
  ok.
command(ClusterMember, Command) ->
  raft_server:command(ClusterMember, Command).

-spec join(pid(), pid()) -> ok.
join(ActualClusterMember, NewClusterMember) ->
  raft_server:join(ActualClusterMember, NewClusterMember).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  raft_server:leave(ClusterMember, MemberToLeave).

-spec status(pid()) -> {follower | candidate | leader, term(), pid(), [pid()]}.
status(ClusterMember) ->
  raft_server:status(ClusterMember).