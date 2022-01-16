%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2022, Peter Tihanyi
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_cluster).
-author("Peter Tihanyi").

-include("raft.hrl").

-record(raft_cluster, {
    members = [] :: [pid()],
    leader = undefined :: pid() | undefined,
    majority = 0 :: pos_integer()
    }).

-type(cluster() :: #raft_cluster{}).

-export_type([cluster/0]).

%% API
-export([new/0,
         leave/2, join/2,
         members/1, is_member/2,
         majority_count/1, member_count/1,
         leader/2, leader/1,
         joint_cluster/2]).

-spec new() -> cluster().
new() ->
  #raft_cluster{members = [self()], majority = 1}.

-spec is_member(pid(), cluster()) -> boolean().
is_member(Member, #raft_cluster{members = Members}) ->
  lists:member(Member, Members).

-spec leader(cluster()) -> pid() | undefined.
leader(#raft_cluster{leader = Leader}) ->
  Leader.

-spec leader(cluster(), pid() | undefined) -> cluster().
leader(Cluster, undefined) ->
  Cluster#raft_cluster{leader = undefined};
leader(Cluster, Leader) ->
  case is_member(Leader, Cluster) of
    true ->
      Cluster#raft_cluster{leader = Leader};
    false ->
      Cluster#raft_cluster{leader = undefined}
  end.

-spec joint_cluster(cluster(), cluster()) -> cluster().
joint_cluster(#raft_cluster{members = CurrentClusterMembers} = Cluster,
              #raft_cluster{members = NewClusterMembers}) ->
  JointClusterMembers = lists:usort(CurrentClusterMembers ++ NewClusterMembers),
  update_majority(Cluster#raft_cluster{members = JointClusterMembers}).

-spec members(cluster()) -> [pid()].
members(#raft_cluster{members = Members}) ->
    Members.

-spec majority_count(cluster()) -> pos_integer().
majority_count(#raft_cluster{majority = Majority}) ->
  Majority.

-spec update_majority(cluster()) -> cluster().
update_majority(Cluster) ->
  Cluster#raft_cluster{majority = (member_count(Cluster) div 2) + 1}.

-spec join(pid(), cluster()) -> {ok, cluster()} | {error, already_member}.
join(Member, #raft_cluster{members = Members} = State) ->
  case lists:member(Member, Members) of
    true ->
      {error, already_member};
    _ ->
      {ok, update_majority(State#raft_cluster{members = [Member | Members]})}
  end.

-spec leave(pid(), cluster()) ->
  {ok, cluster()} | {error, not_member | last_member_in_the_cluster}.
leave(Member, #raft_cluster{members = Members} = State) ->
  case lists:member(Member, Members) of
    false ->
      {error, not_member};
    _ when Members =:= [Member] ->
      {error, last_member_in_the_cluster};
    _ ->
      NewCluster = update_majority(State#raft_cluster{members = lists:delete(Member, Members)}),
      NewCluster2 = case Member =:= leader(NewCluster) of
                      true ->
                        leader(NewCluster, undefined);
                      _ ->
                        NewCluster
                    end,
      {ok, NewCluster2}
  end.

-spec member_count(cluster()) -> pos_integer().
member_count(#raft_cluster{members = Members}) ->
    length(Members).