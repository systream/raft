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
-export([leave/2, join/2, members/1, new/0, majority_count/1, member_count/1, leader/2, leader/1]).

-spec new() -> cluster().
new() ->
    update_majority(#raft_cluster{members = [self()]}).

-spec leader(cluster()) -> pid() | undefined.
leader(#raft_cluster{leader = Leader}) ->
    Leader.

-spec leader(cluster(), pid() | undefined) -> cluster().
leader(Cluster, Leader) ->
    Cluster#raft_cluster{leader = Leader}.

-spec members(cluster()) -> [pid()].
members(#raft_cluster{members = Members}) ->
    Members.

-spec majority_count(cluster()) -> pos_integer().
majority_count(#raft_cluster{majority = Majority}) ->
    Majority.

-spec update_majority(cluster()) -> cluster().
update_majority(Cluster) ->
    Cluster#raft_cluster{majority = (member_count(Cluster) div 2) + 1}.

-spec join(pid(), cluster()) -> {ok, cluster()}.
join(Member, #raft_cluster{members = Members} = State) ->
    case lists:member(Member, Members) of
        true ->
            {error, already_member};
        _ ->
            {ok, update_majority(State#raft_cluster{members = [Member | Members]})}
    end.

-spec leave(pid(), cluster()) -> cluster().
leave(Member, #raft_cluster{members = Members} = State) ->
    case lists:member(Member, Members) of
        false ->
            {error, not_member};
        _ when Member =/= self() ->
            {ok, update_majority(State#raft_cluster{members = lists:delete(Member, Members)})};
        _ when Member =:= self() ->
            {ok, new()}
    end.

-spec member_count(cluster()) -> pos_integer().
member_count(#raft_cluster{members = Members}) ->
    length(Members).