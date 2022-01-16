-module(prop_raft).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3,
         stop_collaborator/1, add_collaborator/1, kill_collaborator/1]).

-record(state, {
  collaborators = [] :: [pid()],
  storage = #{}
}).

add_collaborator(ClusterMember) ->
  {ok, Pid} = raft:start(raft_test_cb),
  case catch raft:join(ClusterMember, Pid) of
    ok ->
      {ok, Pid};
    Else ->
      Else
  end.

stop_collaborator(Pid) ->
  raft:stop(Pid).

kill_collaborator(Pid) ->
  exit(Pid, kill),
  ok.


%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
  application:ensure_all_started(raft),
  setup_config(),
  ?FORALL(Cmds, commands(?MODULE, initial_state()),
          begin
              {History, State, Result} = run_commands(?MODULE, Cmds),
              %CompareStates = compare_states(State#state.collaborators),
              %[raft:stop(Pid) || Pid <- State#state.collaborators],
              ?WHENFAIL(
                begin
                  io:format("History: ~p\nState: ~p\nResult: ~p\n", [History,State,Result])
                end,
                aggregate(command_names(Cmds),
                          Result =:= ok
                          %andalso CompareStates =:= ok
                ))
          end).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
  {ok, Pid} = raft:start(raft_test_cb),
  setup_config(),
  Collaborators = [Pid],
  #state{collaborators = Collaborators}.

%% @doc List of possible commands to run against the system
command(#state{collaborators = Collaborators} = _State) ->
  frequency([
    {50, {call, raft, command, [oneof(Collaborators), {store, store_key(), pos_integer()}]}},
    {4, {call, ?MODULE, add_collaborator, [oneof(Collaborators)]}},
    {3, {call, raft, leave, [oneof(Collaborators), oneof(Collaborators)]}},
    {1, {call, ?MODULE, kill_collaborator, [oneof(Collaborators)]}},
    {1, {call, ?MODULE, stop_collaborator, [oneof(Collaborators)]}}
  ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(#state{collaborators = [_Item]}, {call, raft, leave, [_]}) ->
  false;
precondition(#state{collaborators = [_Item]}, {call, ?MODULE, kill_collaborator, [_]}) ->
  false;
precondition(#state{collaborators = [_Item]}, {call, ?MODULE, stop_collaborator, [_]}) ->
  false;
precondition(#state{}, {call, raft, command, _}) ->
  true;
precondition(#state{}, {call, ?MODULE, add_collaborator, _}) ->
  true;
precondition(#state{collaborators = Collaborators},
             {call, _Module, _Function, [On | _]} = C) ->
  case lists:member(On, Collaborators) of
    false ->
      io:format(user, "call on ~p -> ~p : ~p~n", [C, Collaborators, false]);
    _ ->
      ok
  end,
  lists:member(On, Collaborators);
precondition(#state{}, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(#state{storage = Storage, collaborators = Cluster} = _State,
              {call, raft, command, [On, {store, Key, Value}]}, {ok, StoredValue}) ->
  StorageGet = maps:get(Key, Storage, 0),
  case lists:member(On, Cluster) of
    true ->
      case raft:query(On, {get, Key}) of
        GetValue when GetValue >= (StorageGet+Value) ->
          true;
        Else ->
          io:format(user, "Value should be ~p of key \"~p\" instead of ~p on node ~p~n ~p -> ~p~n",
                    [Value, Key, Else, On, raft:status(On), _State]),
          false
      end;
    _ ->
      true
  end;
postcondition(#state{} = _State, {call, raft, command, [_On, _]}, {error, no_leader}) ->
  true;
postcondition(#state{} = _State, {call, raft, command, [_On, _]}, {error, leader_changed}) ->
  true;
postcondition(_State, {call, ?MODULE, add_collaborator, [_On]}, {ok, _NewMember}) ->
  true;
postcondition(_State, {call, ?MODULE, add_collaborator, [_On]}, {error, no_leader}) ->
  true;
postcondition(_State, {call, ?MODULE, add_collaborator, [_On]}, {error, leader_changed}) ->
  true;
postcondition(_State, {call, raft, leave, [_On, _Target]}, ok) ->
  true;
postcondition(_State, {call, raft, leave, [_On, _Target]}, {error, not_member}) ->
  true;
postcondition(_State, {call, raft, leave, [_On, _Target]}, {error, no_leader}) ->
  true;
postcondition(_State, {call, raft, leave, [_On, _Target]}, {error, last_member_in_the_cluster}) ->
  true;
postcondition(_State, {call, ?MODULE, kill_collaborator, [_Pid]}, ok) ->
  true;
postcondition(_State, {call, ?MODULE, stop_collaborator, [_Pid]}, ok) ->
  true;
postcondition(_State, {call, _Mod, _Fun, _Args} = A, _Res) ->
  io:format("unhandled post condition: ~p~n~p~n", [A, _Res]),
  false.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#state{storage = Storage, collaborators = InCluster} = State, {ok, Value},
           {call, raft, command, [On, {store, Key, _Value}]}) ->
  case lists:member(On, InCluster) of
    true ->
      State#state{storage = Storage#{Key => Value}};
    _ ->
      State
  end;

next_state(#state{collaborators = InCluster} = State, {ok, NewMember},
           {call, ?MODULE, add_collaborator, [_On]}) ->
  State#state{collaborators = [NewMember | InCluster]};

next_state(#state{collaborators = InCluster} = State, ok,
           {call, raft, leave, [_On, Target]}) ->
  State#state{collaborators = lists:delete(Target, InCluster)};

next_state(#state{collaborators = InCluster} = State, ok,
           {call, ?MODULE, kill_collaborator, [Target]}) ->
  State#state{collaborators = lists:delete(Target, InCluster)};

next_state(#state{collaborators = InCluster} = State, ok,
           {call, ?MODULE, stop_collaborator, [Target]}) ->
  State#state{collaborators = lists:delete(Target, InCluster)};

next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    State.

store_key() ->
  integer(1, 100).

setup_config() ->
  Level = warning,
  Config = #{ config => #{drop_mode_qlen => 1000000000,
                          file => "log/raft.log",
                          burst_limit_enable => false,
                          flush_qlen => 1000000000,
                          max_no_bytes => 10485760,
                          max_no_files => 5,
                          sync_mode_qlen => 100},
              formatter => {logger_formatter,
                            #{max_size => 200000,
                              template => [time, " " , level, " [", pid, ",", raft_role, "] ", msg, "\n"]
                            }
              },
              level => Level
           },
  logger:add_handler(raft, logger_std_h, Config),
  logger:set_primary_config(level, Level),
  Meta = #{raft_role => proper_tester},
  NewMeta = case logger:get_process_metadata() of
              undefined ->
                Meta;
              OldMeta ->
                maps:merge(OldMeta, Meta)
            end,
  logger:set_process_metadata(NewMeta).


compare_states([]) ->
  ok;
compare_states(Servers) ->
  raft:command(hd(Servers), noop), % to execute last state chaining command on followers
  compare_states(Servers, 3).

compare_states(Servers, MaxWaits) ->
  %io:format(user, "Comparing user states ~n", []),
  UserStates = get_user_states(Servers),
  case do_compare_term([UserState || {_, UserState} <- UserStates]) of
    ok ->
      ok;
    diff when MaxWaits > 0 ->
      %io:format(user, "Found diff but may not catch up the follower, waiting ~n", []),
      timer:sleep(100),
      compare_states(Servers, MaxWaits-1);
    diff ->
      DiffPr = [{Server, last_log_index(Server), UserState, raft:status(Server)} ||
                 {Server, UserState} <- UserStates],
      io:format(user, "Diff: ~p ~n", [DiffPr]),
      {diff, DiffPr}
  end.

do_compare_term([Base | Rest]) ->
  compare_term(Rest, Base).

compare_term([], _Base) ->
  ok;
compare_term([Base | Rest], Base) ->
  compare_term(Rest, Base);
compare_term([Diff | _Rest], Base) when Diff =/= Base ->
  diff.

get_user_states(Servers) ->
  [begin
     {_, State} = sys:get_state(Server),
     {Server, erlang:element(3, State)}
   end || Server <- Servers].

last_log_index(Server) ->
  #{log_last_index := La} = raft:status(Server),
  La.
