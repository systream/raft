%%%-------------------------------------------------------------------
%%% @author Peter Tihanti
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_test_cb).
-author("Peter Tihanti").

-behavior(raft_collaborator).

%% API
-export([init/0, execute/2]).


-spec init() -> State when State :: term().
init() ->
  #{}.

-spec execute(Command, State) -> {reply, Reply, State} when
  Command :: term(),
  State :: term(),
  Reply :: term().
execute({store, Key, Value}, State) ->
  NewValue = maps:get(Key, State, 0)+Value,
  {reply, NewValue, State#{Key => NewValue}};
execute({get, Key}, State) ->
  {reply, maps:get(Key, State, 0), State}.