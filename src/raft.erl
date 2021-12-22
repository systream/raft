%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft).
-author("Peter Tihanyi").

-export([start/1, join/2, leave/2, status/1, command/2, test/0, test_mult/0]).


test_mult() ->
  {Ok, []} = rpc:multicall(raft, start, [raft_process_registry]),
  [{ok, FirstPid} | Rest] = Ok,
  [ok = raft:join(FirstPid, Pid)  || {ok, Pid} <- Rest],
  FirstPid.

test() ->
  {ok, A} = start(raft_test_cb),
  {ok, B} = start(raft_test_cb),
  {ok, C} = start(raft_test_cb),
  {ok, D} = start(raft_test_cb),
  {ok, E} = start(raft_test_cb),
  join(C, D),
  [spawn(fun() -> [command(A, {store, {I, X}, I}) || I <- lists:seq(1, 5000)] end)
   || X <- lists:seq(1, 5)],
  timer:sleep(5000),
  join(B, A),
  join(A, C),
  join(A, D),
  join(A, E),
  timer:sleep(5000),
  StatusA = {_Type, _Term, _Leader, Collaborators} = status(A),
  print(StatusA),
  [print(status(Collaborator)) || Collaborator <- Collaborators].

print({Type, Term, Leader, Collaborators}) ->
  io:format(user, "[~p,~p (~p)] -> ~p~n", [Leader, Type, Term, Collaborators]).


-spec start(module()) -> {ok, pid()} | {error, term()}.
start(Callback) ->
  raft_collaborator_sup:start_collaborator(Callback).

-spec command(pid(), term()) ->
  ok.
command(ClusterMember, Command) ->
  raft_collaborator:command(ClusterMember, Command).

-spec join(pid(), pid()) -> ok.
join(ActualClusterMember, NewClusterMember) ->
  raft_collaborator:join(ActualClusterMember, NewClusterMember).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  raft_collaborator:leave(ClusterMember, MemberToLeave).

-spec status(pid()) -> {follower | candidate | leader, term(), pid(), [pid()]}.
status(ClusterMember) ->
  raft_collaborator:status(ClusterMember).