%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_test_cb).
-author("Peter Tihanyi").

-behavior(raft_server).

%% API
-export([init/0, handle_command/2]).


-spec init() -> State when State :: term().
init() ->
  #{}.

-spec handle_command(Command, State) -> {reply, Reply, State} when
  Command :: term(),
  State :: term(),
  Reply :: term().
handle_command({store, Key, Value}, State) ->
  NewValue = maps:get(Key, State, 0)+Value,
  {reply, {ok, NewValue}, State#{Key => NewValue}};
handle_command({get, Key}, State) ->
  {reply, maps:get(Key, State, 0), State}.