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
-export([init/0, handle_command/2, handle_query/2, get_log_module/0]).

get_log_module() ->
  raft_log_ets.

-spec init() -> State when State :: term().
init() ->
  #{}.

-spec handle_command(Command, State) -> {Reply, State} when
  Command :: term(),
  State :: term(),
  Reply :: term().
handle_command({store, Key, Value}, State) ->
  NewState = State#{Key => Value},
  %io:format(user, "[~p] store ~p -> ~p Newtate: ~p~n", [self(), Key, Value, NewState]),
  {ok, NewState};
handle_command({increment, Key, Value}, State) ->
  NewValue = maps:get(Key, State, 0)+Value,
  {{ok, NewValue}, State#{Key => NewValue}};
handle_command({hash, Key, Value}, State) ->
  CValue = maps:get(Key, State, 0),
  NewValue = erlang:phash2(<<CValue/integer, (term_to_binary(Value))/binary>>),
  {{ok, NewValue}, State#{Key => NewValue}};
handle_command(noop, State) ->
  {ok, State};
handle_command(clear_state, _State) ->
  {ok, init()};
handle_command({sleep, Time}, State) ->
  timer:sleep(Time),
  {ok, State};

handle_command({register, Key, Pid}, State) ->
  case maps:get(Key, State, not_registered) of
    not_registered ->
      {ok, State#{Key => Pid}};
    RegPid ->
      {{error, {already_registered, RegPid}}, State}
  end.

-spec handle_query(Command, State) -> Reply when
  Command :: term(),
  State :: term(),
  Reply :: term().
handle_query({get, Key}, State) ->
  %io:format(user, "[~p] get ~p State: ~p~n", [self(), Key, State]),
  maps:get(Key, State, no_value).

