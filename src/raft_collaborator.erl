%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_collaborator).
-author("Peter Tihanyi").

-behaviour(gen_statem).

% election timeouts
-define(MAX_HEARTBEAT_TIMEOUT, 15000).
-define(MIN_HEARTBEAT_TIMEOUT, 5000).
-define(HEARTBEAT_GRACE_TIME, 5000).

-define(HEARTBEAT_STATE_TIMEOUT(Timeout), {state_timeout, Timeout, heartbeat_timeout}).
-define(HEARTBEAT_STATE_TIMEOUT, ?HEARTBEAT_STATE_TIMEOUT(get_timeout())).

%% API
-export([start_link/1, join/2, leave/2, status/1, command/2]).

%% gen_statem callbacks
-export([init/1,
         format_status/2,
         handle_event/4, terminate/3,
         code_change/4, callback_mode/0]).

-define(SERVER, ?MODULE).

-type(state_name() :: follower | candidate | leader).

-record(state, {
  callback :: module(),
  user_state :: term(),
  term = 1 :: pos_integer(),
  collaborators = [] :: [pid()],
  leader :: pid(),
  log :: raft_log:log_ref(),
  votes = 0 :: non_neg_integer(),
  voted_for_term = 1 :: pos_integer()
}).

-record(heartbeat_req, {
  term :: pos_integer(),
  leader :: pid()
}).

-record(change_state_req, {
  term :: pos_integer(),
  collaborators :: [pid()],
  leader :: pid()
}).

-record(append_log_req, {
  log_pos :: pos_integer(),
  command :: term()
}).

-record(append_log_ack, {
  log_pos :: pos_integer(),
  collaborator :: pid()
}).

-record(append_log_nack, {
  log_pos :: pos_integer(),
  collaborator_log_pos :: pos_integer(),
  collaborator :: pid()
}).

-record(catch_up_req, {
  log_pos :: pos_integer(),
  target :: pid()
}).

-callback init() -> State when State :: term().

-callback execute(Command, State) -> {reply, Reply, State} when
  Command :: term(),
  State :: term(),
  Reply :: term().

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(module()) -> {ok, pid()} | ignore | {error, term()}.
start_link(CallbackModule) ->
  gen_statem:start_link(?MODULE, CallbackModule, []).

-spec join(pid(), pid()) -> ok.
join(ActualClusterMember, NewClusterMember) ->
  gen_statem:call(ActualClusterMember, {join, NewClusterMember}).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  gen_statem:call(ClusterMember, {leave, MemberToLeave}).

-spec status(pid()) -> {state_name(), term(), pid(), [pid()]}.
status(ClusterMember) ->
  gen_statem:call(ClusterMember, status).

-spec command(pid(), term()) ->
  ok.
command(ActualClusterMember, Command) ->
  gen_statem:call(ActualClusterMember, {command, Command}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
%% @doc Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
init(CallbackModule) ->
  io:format(user, "[~p] starting ~p~n", [self(), CallbackModule]),
  process_flag(trap_exit, true),
  {ok, follower, #state{collaborators = [self()],
                        callback = CallbackModule,
                        user_state = apply(CallbackModule, init, []),
                        log = raft_log:new()}}.

%% @private
%% @doc This function is called by a gen_statem when it needs to find out
%% the callback mode of the callback module.
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
%% @doc Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
format_status(_Opt, [_PDict, StateName, _State]) ->
  {_Opt, [_PDict, StateName, _State]}.

%% @private
%% @doc If callback_mode is handle_event_function, then whenever a
%% gen_statem receives an event from call/2, cast/2, or as a normal
%% process message, this function is called.

handle_event({call, From}, status, StateName,
             #state{leader = Leader, term = Term, collaborators = Collaborators}) ->
  gen_statem:reply(From, {StateName, Term, Leader, Collaborators}),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%
%% Follower
%% %%%%%%%%%%%%%%%

handle_event(enter, _PrevStateName, follower, #state{collaborators = [Collaborator]} = _State)
  when Collaborator =:= self() -> % alone in cluster, no one will send heartbeat
  io:format(user, "[~p, follower] start follower~n", [self()]),
  {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT(0)]};
handle_event(enter, _PrevStateName, follower, _State) ->
  io:format(user, "[~p, follower] become follower~n", [self()]),
  {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT]};
handle_event(state_timeout, heartbeat_timeout, follower, State) ->
  io:format(user, "[~p, follower] heartbeat timeout~n", [self()]),
  {next_state, candidate, State};

handle_event(cast, #append_log_req{log_pos = NewLogPos, command = Command}, follower,
             #state{log = LogRef, leader = Leader} = State) ->
  Pos = raft_log:get_pos(LogRef),
  case Pos =:= NewLogPos-1 of
    true ->
      NewLogRef = raft_log:append(Command, LogRef),
      gen_statem:cast(Leader, #append_log_ack{log_pos = NewLogPos, collaborator = self()}),
      {keep_state, State#state{log = NewLogRef}, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      gen_statem:cast(Leader, #append_log_nack{log_pos = NewLogPos,
                                               collaborator_log_pos = Pos, collaborator = self()}),
      {keep_state, catch_up(State), [?HEARTBEAT_STATE_TIMEOUT]}
  end;

%% %%%%%%%%%%%%%%%
%% Candidate
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, candidate, State = #state{term = Term}) ->
  NewState = State#state{term = Term+1, votes = 0},
  io:format(user, "[~p, candidate] become candidate~n", [self()]),
  ask_for_vote(NewState, self()),
  {keep_state, NewState, [?HEARTBEAT_STATE_TIMEOUT]};
handle_event(cast, {accept, Term, Voter}, candidate,
          #state{term = Term, collaborators = Collaborators, votes = Votes} = State) ->
  io:format(user, "[~p, candidate] accept ~p ~p~n", [self(), Term, Voter]),
  case lists:member(Voter, Collaborators) of
    true ->
      NewState = State#state{votes = Votes+1},
      case have_majority(NewState) of
        true ->
          {next_state, leader, NewState, [?HEARTBEAT_STATE_TIMEOUT]};
        _ ->
          {keep_state, NewState, [?HEARTBEAT_STATE_TIMEOUT]}
      end;
    _ ->
      % ignore
      keep_state_and_data
  end;
handle_event(state_timeout, heartbeat_timeout, candidate, State) ->
  {repeat_state, State};


%% %%%%%%%%%%%%%%%
%% Leader
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, leader, State) ->
  io:format(user, "[~p, leader] become leader~n", [self()]),
  {keep_state, State#state{leader = self()},
   [{state_timeout, rand:uniform(?MIN_HEARTBEAT_TIMEOUT-10)+10, heartbeat_timeout}]};
handle_event(state_timeout, heartbeat_timeout, leader, State) ->
  send_heartbeat(State),
  {keep_state_and_data, [{state_timeout, ?MIN_HEARTBEAT_TIMEOUT, heartbeat_timeout}]};
handle_event({call, From}, {command, Command}, leader, #state{log = Log,
                                                              collaborators = Collaborators,
                                                              callback = CallbackModule,
                                                              user_state = UserState} = State) ->
  io:format(user, "[~p, leader] command ~p~n", [self(), Command]),
  NewLog = raft_log:append(Command, Log),

  AppendReq = #append_log_req{log_pos = raft_log:get_pos(NewLog), command = Command},
  [gen_statem:cast(Collaborator, AppendReq) || Collaborator <- Collaborators],

  %@TODO: wait for majority replylog

  case apply(CallbackModule, execute, [Command, UserState]) of
    {reply, Reply, NewUserState} ->
      gen_statem:reply(From, Reply),
      {keep_state, State#state{user_state = NewUserState, log = NewLog}, []}
  end;
handle_event({call, From}, #catch_up_req{log_pos = LogPos, target = Pid}, leader,
             #state{log = Log}) ->
  io:format(user, "[~p, leader] stream req ~p -> ~p~n", [self(), From, LogPos]),
  Ref = raft_log:stream(Log, Pid),
  gen_statem:reply(From, Ref),
  keep_state_and_data;

handle_event({call, From}, {join, Pid}, leader, #state{collaborators = Collaborators} = State) ->
  io:format(user, "[~p, leader] join ~p~n", [self(), Pid]),
  NewState = State#state{collaborators = lists:usort([Pid | Collaborators])},
  send_change_state(NewState),
  gen_statem:reply(From, ok),
  {keep_state, NewState, []};
handle_event({call, From}, {leave, Leader}, leader,
             #state{leader = Leader, collaborators = Collaborators} = State) ->
  io:format(user, "[~p, leader] leave leader ~p~n", [self(), Leader]),
  RemoteState = State#state{collaborators = lists:delete(Leader, Collaborators)},
  send_change_state(RemoteState),
  gen_statem:reply(From, ok),
  {next_state, follower, State#state{collaborators = [self()]}, []};
handle_event({call, From}, {leave, Pid}, leader, #state{collaborators = Collaborators} = State) ->
  io:format(user, "[~p, leader] leave ~p~n", [self(), Pid]),
  NewState = State#state{collaborators = lists:delete(Pid, Collaborators)},
  send_change_state(NewState),
  gen_statem:reply(From, ok),
  {keep_state, NewState, []};

handle_event(cast, {proxy, Type, Context}, leader, State) ->
  io:format(user, "[~p, leader] got proxy ~p~n", [self(), {proxy, Type, Context}]),
  handle_event(Type, Context, leader, State);

handle_event(cast, {proxy, _Type, _Context}, _, #state{leader = undefined}) ->
  {keep_state_and_data, [postpone]};
handle_event(cast, {proxy, Type, Context}, _StateName, #state{leader = Leader}) ->
  gen_statem:cast(Leader, {proxy, Type, Context}),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%
%% Heartbeat
%% %%%%%%%%%%%%%%%
handle_event(cast, #heartbeat_req{term = Term, leader = Leader}, _StateName,
             #state{term = Term, leader = Leader}) ->
  {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT]};
handle_event(cast, #heartbeat_req{term = NewTerm, leader = Leader}, _StateName,
             #state{collaborators = Collaborators, term = Term} = State) when NewTerm > Term ->
  case lists:member(Leader, Collaborators) of
    true ->
      io:format("New leader: ~p -> ~p~n", [Leader, State]),
      NewState = new_leader(State#state{term = NewTerm}, Leader),
      {next_state, follower, NewState, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      % drop
      keep_state_and_data
  end;

%% %%%%%%%%%%%%%%%
%% Vote
%% %%%%%%%%%%%%%%%
% vote myself ;)
handle_event(cast, {vote_request, Target, NewTerm}, _StateName,
             #state{voted_for_term = Term} = State)
  when Target =:= self() andalso NewTerm > Term ->
  gen_statem:cast(Target, {accept, NewTerm, self()}),
  {keep_state, State#state{voted_for_term = NewTerm}};
handle_event(cast, {vote_request, Target, NewTerm}, _StateName,
             #state{collaborators = Collaborators, voted_for_term = Term} = State)
  when NewTerm > Term ->
  case lists:member(Target, Collaborators) of
    true ->
      gen_statem:cast(Target, {accept, NewTerm, self()}),
      {keep_state, State#state{voted_for_term = NewTerm}, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      % drop
      keep_state_and_data
  end;
handle_event(cast, {vote_request, Target, NewTerm}, _StateName,
             #state{collaborators = Collaborators} = _State) ->
  case lists:member(Target, Collaborators) of
    true ->
      gen_statem:cast(Target, {reject, NewTerm, self()}),
      {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      % drop
      keep_state_and_data
  end;

%% proxy requests to leader
handle_event({call, _From}, _Request, _StateName, #state{leader = undefined} = _State) ->
  io:format(user, "[~p, ~p] postpone proxy req ~p~n", [self(), _StateName, _Request]),
  {keep_state_and_data, [postpone]};

handle_event({call, From}, {command, Command}, _StateName, #state{leader = Leader} = _State) ->
  io:format(user, "[~p, ~p] proxy req ~p~n", [self(), _StateName, {command, Command}]),
  gen_statem:cast(Leader, {proxy, {call, From}, {command, Command}}),
  keep_state_and_data;
handle_event({call, From}, {join, Pid}, _StateName, #state{leader = Leader} = _State) ->
  io:format(user, "[~p, ~p] proxy req ~p~n", [self(), _StateName, {join, Pid}]),
  gen_statem:cast(Leader, {proxy, {call, From}, {join, Pid}}),
  keep_state_and_data;
handle_event({call, From}, {leave, Pid}, _StateName, #state{leader = Leader} = _State) ->
  io:format(user, "[~p, ~p] proxy req ~p~n", [self(), _StateName, {leave, Pid}]),
  gen_statem:cast(Leader, {proxy, {call, From}, {leave, Pid}}),
  keep_state_and_data;

% state change req
handle_event(cast, #change_state_req{term = Term, collaborators = Collaborators, leader = Leader},
             _, #state{} = State) ->
  case lists:member(self(), Collaborators) of
    true ->
      new_leader(State, Leader),
      {next_state, follower, State#state{term = Term, collaborators = Collaborators,
                                         leader = Leader, votes = 0, voted_for_term = Term}};
    _ ->
      new_leader(State, undefined),
      {next_state, follower, State#state{term = Term, collaborators = [self()], leader = undefined,
                                         votes = 0, voted_for_term = Term}}
  end;


handle_event(info, {'EXIT', Leader, _Type}, _StateName,
             #state{collaborators = Collaborators, leader = Leader} = State) ->
  % leader died
  NewCollaborators = lists:delete(Leader, Collaborators),
  {keep_state, State#state{collaborators = NewCollaborators}, ?HEARTBEAT_STATE_TIMEOUT(0)};
handle_event(info, {'EXIT', Pid, _Type}, _StateName,
             #state{collaborators = Collaborators} = State) ->
  io:format(user, "[~p, ~p] got exit from ~p~n", [self(), _StateName, Pid]),
  {keep_state, State#state{collaborators = lists:delete(Pid, Collaborators)}};
handle_event(_EventType, _EventContent, StateName, State) ->
  io:format(user, "follower unhandled: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n",
            [_EventType, _EventContent, State, StateName]),
  keep_state_and_data.

%% @private
%% @doc This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
terminate(_Reason, _StateName, _State = #state{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State = #state{}, _Extra) ->
  {ok, StateName, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
have_majority(#state{collaborators = Collaborators, votes = Votes}) ->
  ((length(Collaborators) div 2) + 1) =< Votes.

ask_for_vote(#state{term = Term, collaborators = Collaborators}, Target) ->
  [gen_server:cast(Pid, {vote_request, Target, Term}) || Pid <- Collaborators],
  ok.

send_heartbeat(#state{collaborators = Collaborators, term = Term, leader = Leader}) ->
  Req = #heartbeat_req{leader = Leader, term = Term},
  [gen_server:cast(Pid, Req) || Pid <- Collaborators, Pid =/= self()].

send_change_state(#state{collaborators = Collaborators, term = Term, leader = Leader}) ->
  ChangeState = #change_state_req{collaborators = Collaborators, term = Term, leader = Leader},
  [gen_server:cast(Pid, ChangeState) || Pid <- Collaborators, Pid =/= self()].

get_timeout() ->
  Rand = rand:uniform(?MAX_HEARTBEAT_TIMEOUT-?MIN_HEARTBEAT_TIMEOUT),
  ?MIN_HEARTBEAT_TIMEOUT+Rand+?HEARTBEAT_GRACE_TIME.

new_leader(#state{leader = undefined} = State, undefined) ->
  State;
new_leader(#state{leader = undefined} = State, NewLeader) ->
  link(NewLeader),
  State#state{leader = NewLeader};
new_leader(#state{leader = Leader} = State, NewLeader) when is_pid(Leader) ->
  unlink(Leader),
  new_leader(State#state{leader = undefined}, NewLeader).

catch_up(#state{leader = Leader, log = Log} = State) ->
  Ref = gen_statem:call(Leader, #catch_up_req{log_pos = raft_log:get_pos(Log), target = self()}),
  {ok, NewLog} = read_log_stream(Ref, Log),
  State#state{log = NewLog}.

read_log_stream(Ref, Log) ->
  case raft_log:read_stream(Ref, 35000) of
    {ok, _Pos, Data} ->
      NewLog = raft_log:append(Data, Log),
      read_log_stream(Ref, NewLog);
    '$end_of_stream' ->
      {ok, Log};
    timeout ->
      timeout
  end.