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

-record(cluster, {
    members = [] :: [pid()],
    majority = 0 :: pos_integer()
    }).

-type(cluster() :: #cluster{}).

-export_type([cluster/0]).

%% API
-export([leave/2, join/2, members/1, new/0, majority_count/1, member_count/1]).

-spec new() -> cluster().
new() ->
    update_majority(#cluster{members = [self()]}).

-spec members(cluster()) -> [pid()].
members(#cluster{members = Members}) ->
    Members.

-spec majority_count(cluster()) -> pos_integer().
majority_count(#cluster{majority = Majority}) ->
    Majority.

-spec update_majority(cluster()) -> cluster().
update_majority(Cluster) ->
    Cluster#cluster{majority = (member_count(Cluster) div 2) + 1}.

-spec join(pid(), cluster()) -> {ok, cluster()}.
join(Member, #cluster{members = Members} = State) ->
    case lists:member(Member, Members) of
        true ->
            {error, already_member};
        _ ->
            ?LOG("[~p] Member added ~p~n", [self(), Member]),
            {ok, update_majority(State#cluster{members = [Member | Members]})}
    end.

-spec leave(pid(), cluster()) -> cluster().
leave(Member, #cluster{members = Members} = State) ->
    case maps:is_key(Member, Members) of
        false ->
            {error, not_member};
        _ when Member =/= self() ->
            ?LOG("[~p] Member removed ~p~n", [self(), Member]),
            {ok, update_majority(State#cluster{members = lists:delete(Member, Members)})};
        _ when Member =:= self() ->
            ?LOG("[~p] Member ramins alone in cluster ~p ~n", [self(), Member]),
            {ok, new()}
    end.

-spec member_count(cluster()) -> pos_integer().
member_count(#cluster{members = Members}) ->
    length(Members).