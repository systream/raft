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

%-define(LOG(Msg, Args), io:format(user, "[~p] " ++ Msg, [erlang:system_time(millisecond) | Args])).
-define(LOG(_Msg, _Args), ok).

% election timeouts
-define(MAX_HEARTBEAT_TIMEOUT, 35000).
-define(MIN_HEARTBEAT_TIMEOUT, 15000).
-define(HEARTBEAT_GRACE_TIME, 5000).

-define(CONSENSUS_TIMEOUT, 5000).

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

-record(candidate_state, {
  state :: #state{},
  votes_ack = 0 :: non_neg_integer(),
  votes_nack = 0 :: non_neg_integer()
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

-record(execute_log_req, {
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

-record(vote_req, {
  candidate :: pid(),
  term :: pos_integer()
}).

-record(vote_ack, {
  term :: pos_integer(),
  voter = self() :: pid()
}).

-record(vote_nack, {
  term :: pos_integer(),
  voter = self() :: pid()
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
join(ActualClusterMember, NewClusterMember) when is_pid(NewClusterMember) ->
  gen_statem:call(ActualClusterMember, {join, NewClusterMember}, 30000).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  gen_statem:call(ClusterMember, {leave, MemberToLeave}, 30000).

-spec status(pid()) -> {state_name(), term(), pid(), [pid()]}.
status(ClusterMember) ->
  gen_statem:call(ClusterMember, status, 30000).

-spec command(pid(), term()) ->
  ok.
command(ActualClusterMember, Command) ->
  gen_statem:call(ActualClusterMember, {command, Command}, 30000).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
%% @doc Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
init(CallbackModule) ->
  erlang:process_flag(trap_exit, true),
  ?LOG("[~p] starting ~p~n", [self(), CallbackModule]),
  {ok, follower, init_user_state(#state{collaborators = [self()],
                                        callback = CallbackModule,
                                        log = raft_log:new()})}.

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

%% %%%%%%%%%%%%%%%
%% Follower
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, follower, #state{collaborators = [Collaborator]} = _State)
  when Collaborator =:= self() -> % alone in cluster, no one will send heartbeat
  ?LOG("[~p, follower] start follower~n", [self()]),
  {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT(0)]};
handle_event(enter, _PrevStateName, follower, State) ->
  ?LOG("[~p, follower] become follower and catch_up~n", [self()]),
  {keep_state, catch_up(State), [?HEARTBEAT_STATE_TIMEOUT]};
handle_event(state_timeout, heartbeat_timeout, follower, State) ->
  ?LOG("[~p, follower] heartbeat timeout~n", [self()]),
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
handle_event(cast, #execute_log_req{log_pos = ActualLogPos, command = Command}, follower,
             #state{log = LogRef} = State) ->
  case raft_log:get_pos(LogRef) =:= ActualLogPos of
    true ->
      {reply, _, NewState} = execute_log(Command, State),
      {keep_state, NewState, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      {keep_state, catch_up(State), [?HEARTBEAT_STATE_TIMEOUT]}
  end;


%% %%%%%%%%%%%%%%%
%% Leader
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, leader, State) ->
  ?LOG("[~p, leader] become leader~n", [self()]),
  {keep_state, State#state{leader = self()},
   [{state_timeout, get_min_timeout(), heartbeat_timeout}]};
handle_event(state_timeout, heartbeat_timeout, leader, State) ->
  send_heartbeat(State),
  {keep_state_and_data, [{state_timeout, get_min_timeout(), heartbeat_timeout}]};

handle_event({call, From}, {command, Command}, leader,
             #state{log = Log, collaborators = Collaborators} = State) ->
  ?LOG("[~p, leader] command ~p~n", [self(), Command]),
  NewLog = raft_log:append(Command, Log),

  LogPos = raft_log:get_pos(NewLog),
  AppendReq = #append_log_req{log_pos = LogPos, command = Command},
  [gen_statem:cast(Collaborator, AppendReq) || Collaborator <- Collaborators, Collaborator =/= self()],

  %@TODO: better
  case log_append_majority(LogPos, get_majority_number(length(Collaborators)), 1, 0) of
    true ->
      case execute_log(Command, State) of
        {reply, Reply, NewState} ->
          gen_statem:reply(From, Reply),
          ExecuteLogReq = #execute_log_req{log_pos = LogPos, command = Command},
          [gen_statem:cast(Collaborator, ExecuteLogReq) || Collaborator <- Collaborators, Collaborator =/= self()],
          {keep_state, NewState#state{log = NewLog}, []}
      end;
    false ->
      gen_statem:reply(From, {error, no_majority}),
      keep_state_and_data
  end;

handle_event({call, From}, #catch_up_req{log_pos = LogPos, target = Pid}, leader,
             #state{log = Log}) ->
  ?LOG("[~p, leader] stream req ~p -> ~p~n", [self(), From, LogPos]),
  gen_statem:reply(From, raft_log:stream(raft_log:set_log_pos(Log, LogPos), Pid)),
  keep_state_and_data;

handle_event({call, From}, {join, Pid}, leader, #state{collaborators = Collaborators} = State) ->
  ?LOG("[~p, leader] join ~p~n", [self(), Pid]),
  NewState = State#state{collaborators = lists:usort([Pid | Collaborators])},
  send_change_state(NewState),
  gen_statem:reply(From, ok),
  {keep_state, NewState, []};
handle_event({call, From}, {leave, Leader}, leader,
             #state{leader = Leader, collaborators = Collaborators} = State) ->
  ?LOG("[~p, leader] leave leader ~p~n", [self(), Leader]),
  RemoteState = State#state{collaborators = lists:delete(Leader, Collaborators)},
  send_change_state(RemoteState),
  gen_statem:reply(From, ok),
  {next_state, follower, State#state{collaborators = [self()]}, []};
handle_event({call, From}, {leave, Pid}, leader, #state{collaborators = Collaborators} = State) ->
  ?LOG("[~p, leader] leave ~p~n", [self(), Pid]),
  NewState = State#state{collaborators = lists:delete(Pid, Collaborators)},
  send_change_state(NewState, Collaborators),
  gen_statem:reply(From, ok),
  {keep_state, NewState, []};

handle_event(cast, {proxy, Type, Context}, leader, State) ->
  ?LOG("[~p, leader] got proxy ~p~n", [self(), {proxy, Type, Context}]),
  handle_event(Type, Context, leader, State);

%% %%%%%%%%%%%%%%%
%% Candidate
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, candidate,
             State = #state{term = Term, collaborators = Collaborators}) ->
  ?LOG("[~p, candidate] become candidate~n", [self()]),
  NewTerm = Term+1,
  VoteReq = #vote_req{candidate = self(), term = NewTerm},
  [gen_statem:cast(Collaborator, VoteReq) || Collaborator <- Collaborators],
  {keep_state, #candidate_state{state = State#state{term = NewTerm}}, [?HEARTBEAT_STATE_TIMEOUT]};
handle_event(cast, #vote_ack{term = Term, voter = Voter}, candidate,
             #candidate_state{state = #state{term = Term, collaborators = Collaborators}} = State) ->
  ?LOG("[~p, candidate] accept ~p ~p  -> ~p ~n", [self(), Term, Voter, Collaborators]),
  case lists:member(Voter, Collaborators) of
    true ->
      NewState = State#candidate_state{votes_ack = State#candidate_state.votes_ack+1},
      case have_majority(NewState) of
        true ->
          {next_state, leader, NewState#candidate_state.state, [?HEARTBEAT_STATE_TIMEOUT]};
        _ ->
          {keep_state, NewState, [?HEARTBEAT_STATE_TIMEOUT]}
      end;
    _ ->
      % ignore
      keep_state_and_data
  end;
handle_event(cast, #vote_nack{term = Term, voter = Voter}, candidate,
             #candidate_state{state = #state{term = Term, collaborators = Collaborators}} = State) ->
  ?LOG("[~p, candidate] decline ~p ~p  -> ~p ~n", [self(), Term, Voter, Collaborators]),
  case lists:member(Voter, Collaborators) of
    true ->
      NewState = State#candidate_state{votes_nack = State#candidate_state.votes_nack +1},
      case have_majority(NewState) of
        false ->
          {repeat_state, NewState#candidate_state.state};
        _ ->
          {keep_state, NewState, [?HEARTBEAT_STATE_TIMEOUT]}
      end;
    _ ->
      % ignore
      keep_state_and_data
  end;
handle_event(state_timeout, heartbeat_timeout, candidate, State) ->
  {repeat_state, State#candidate_state.state};

% no leader currently
handle_event(cast, {proxy, _Type, _Context}, _, #state{leader = undefined}) ->
  {keep_state_and_data, [postpone]};
% now we have leader, but unfortunately we need to forward it
handle_event(cast, {proxy, Type, Context}, _StateName, #state{leader = Leader}) ->
  gen_statem:cast(Leader, {proxy, Type, Context}),
  keep_state_and_data;

handle_event(EventType, EventContent, candidate = StateName,
             #candidate_state{state = State} = CandidateState) ->
  case handle_common_event(EventType, EventContent, StateName, State) of
    {keep_state, #state{} = NewState, Actions} ->
      {keep_state, CandidateState#candidate_state{state = NewState}, Actions};
    {keep_state, #state{} = NewState} ->
      {keep_state, CandidateState#candidate_state{state = NewState}};
    {repeat_state, #state{} = NewState} ->
      {repeat_state, CandidateState#candidate_state{state = NewState}};
    {next_state, StateName, #state{} = NewState, Actions} ->
      {next_state, StateName, CandidateState#candidate_state{state = NewState}, Actions};
    {next_state, StateName, #state{} = NewState} ->
      {next_state, StateName, CandidateState#candidate_state{state = NewState}};
    Else ->
      Else
  end;
handle_event(EventType, EventContent, StateName, State) ->
  handle_common_event(EventType, EventContent, StateName, State).


handle_common_event({call, From}, status, StateName,
             #state{leader = Leader, term = Term, collaborators = Collaborators}) ->
  gen_statem:reply(From, {StateName, Term, Leader, Collaborators}),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%
%% Proxy to leader
%% %%%%%%%%%%%%%%%
handle_common_event({call, _From}, _Request, _StateName, #state{leader = undefined} = _State) ->
  ?LOG("[~p, ~p] postpone proxy req ~p~n", [self(), _StateName, _Request]),
  {keep_state_and_data, [postpone]};

handle_common_event({call, From}, {command, Command}, _StateName, #state{leader = Leader} = _State) ->
  ?LOG("[~p, ~p] proxy req ~p~n", [self(), _StateName, {command, Command}]),
  gen_statem:cast(Leader, {proxy, {call, From}, {command, Command}}),
  keep_state_and_data;
handle_common_event({call, From}, {join, Pid}, _StateName, #state{leader = Leader} = _State) ->
  ?LOG("[~p, ~p] proxy req ~p~n", [self(), _StateName, {join, Pid}]),
  gen_statem:cast(Leader, {proxy, {call, From}, {join, Pid}}),
  keep_state_and_data;
handle_common_event({call, From}, {leave, Pid}, _StateName, #state{leader = Leader} = _State) ->
  ?LOG("[~p, ~p] proxy req ~p~n", [self(), _StateName, {leave, Pid}]),
  gen_statem:cast(Leader, {proxy, {call, From}, {leave, Pid}}),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%
%% Heartbeat
%% %%%%%%%%%%%%%%%
handle_common_event(cast, #heartbeat_req{term = Term, leader = Leader}, _StateName,
             #state{term = Term, leader = Leader}) ->
  {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT]};
handle_common_event(cast, #heartbeat_req{term = NewTerm, leader = Leader}, _StateName,
             #state{collaborators = Collaborators, term = Term, log = Log} = State)
  when NewTerm > Term ->
  case lists:member(Leader, Collaborators) of
    true ->
      ?LOG("New leader: ~p -> ~p~n", [Leader, State]),
      NewState = State#state{term = NewTerm,
                             log = raft_log:set_log_pos(Log, 0),
                             leader = Leader},
      {next_state, follower, NewState, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      % drop
      keep_state_and_data
  end;

%% %%%%%%%%%%%%%%%
%% Vote
%% %%%%%%%%%%%%%%%
% vote myself ;)
handle_common_event(cast, #vote_req{candidate = Candidate, term = NewTerm}, _StateName,
             #state{voted_for_term = Term} = State)
  when Candidate =:= self() andalso NewTerm > Term ->
  gen_statem:cast(Candidate, #vote_ack{term = NewTerm}),
  {keep_state, State#state{voted_for_term = NewTerm}};
handle_common_event(cast, #vote_req{candidate = Candidate, term = NewTerm}, _StateName,
             #state{collaborators = Collaborators, voted_for_term = Term} = State)
  when NewTerm > Term ->
  case lists:member(Candidate, Collaborators) of
    true ->
      gen_statem:cast(Candidate, #vote_ack{term = NewTerm}),
      {keep_state, State#state{voted_for_term = NewTerm}, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      % drop
      keep_state_and_data
  end;
handle_common_event(cast, #vote_req{candidate = Candidate, term = NewTerm}, _StateName,
             #state{collaborators = Collaborators} = _State) ->
  case lists:member(Candidate, Collaborators) of
    true ->
      gen_statem:cast(Candidate, #vote_nack{term = NewTerm}),
      {keep_state_and_data, [?HEARTBEAT_STATE_TIMEOUT]};
    _ ->
      % drop
      keep_state_and_data
  end;

% state change req
handle_common_event(cast,
             #change_state_req{term = Term, collaborators = Collaborators, leader = Leader} = Cr,
             _, #state{log = Log} = State) ->
  ?LOG("[~p] state change req: ~p~n", [self(), Cr]),
  case lists:member(self(), Collaborators) of
    true ->
      NewState = init_user_state(State#state{term = Term, collaborators = Collaborators,
                                             leader = Leader, votes = 0, voted_for_term = Term,
                                             log = raft_log:set_log_pos(Log, 0)}),
      link_collaborators(State, NewState),
      {next_state, follower, NewState};
    _ ->
      NewState = init_user_state(State#state{term = Term, collaborators = [self()], leader = undefined,
                                             votes = 0, voted_for_term = Term,
                                             log = raft_log:set_log_pos(Log, 0)}),
      link_collaborators(State, NewState),
      {next_state, follower, NewState}
  end;
handle_common_event(info, {'EXIT', Leader, _Type}, _StateName,
             #state{collaborators = Collaborators, leader = Leader} = State) ->
  % leader died
  NewCollaborators = lists:delete(Leader, Collaborators),
  {keep_state, State#state{collaborators = NewCollaborators},
   ?HEARTBEAT_STATE_TIMEOUT(rand:uniform(10))};

handle_common_event(info, {'EXIT', Pid, _Type}, _StateName,
             #state{collaborators = Collaborators} = State) ->
  ?LOG("[~p, ~p] got exit from ~p~n", [self(), _StateName, Pid]),
  {keep_state, State#state{collaborators = lists:delete(Pid, Collaborators)}};

handle_common_event(info, {'DOWN', _MonitorRef, _Type, Leader, _Info}, _StateName,
             #state{collaborators = Collaborators, leader = Leader} = State) ->
  % leader died
  NewCollaborators = lists:delete(Leader, Collaborators),
  {keep_state, State#state{collaborators = NewCollaborators},
   ?HEARTBEAT_STATE_TIMEOUT(rand:uniform(10))};
handle_common_event(info, {'DOWN', _MonitorRef, _Type, Pid, _Info}, _StateName,
             #state{collaborators = Collaborators} = State) ->
  ?LOG("[~p, ~p] got exit from ~p~n", [self(), _StateName, Pid]),
  {keep_state, State#state{collaborators = lists:delete(Pid, Collaborators)}};
handle_common_event(_EventType, #heartbeat_req{leader = Leader} = _EventContent, Sname, State) ->
  ?LOG("[~p, ~p] unhandled: Et: ~p Ec: ~p~nSt: ~p~n Ls: ~p~n",
       [self(), Sname, _EventType, _EventContent, State, catch sys:get_state(Leader, 10)]),
  keep_state_and_data;
handle_common_event(EventType, EventContent, StateName, #state{leader = undefined} = State) ->
  ?LOG("[~p, ~p] unhandled: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n Leader: ~p~n",
       [self(), StateName, EventType, EventContent, State, StateName, undefined]),
  keep_state_and_data;
handle_common_event(EventType, EventContent, StateName, #state{leader = Leader} = State) ->
  ?LOG("[~p, ~p] unhandled: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n Leader stat: ~p~n",
       [self(), StateName, EventType, EventContent, State, StateName, catch sys:get_state(Leader, 10)]),
  keep_state_and_data.

%% @private
%% @doc This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
terminate(_Reason, _StateName, _State) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State = #state{}, _Extra) ->
  {ok, StateName, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
have_majority(#candidate_state{state = #state{collaborators = Collaborators}} = State) ->
  have_majority(State, get_majority_number(length(Collaborators))).

have_majority(#candidate_state{votes_ack = VotesAck}, Majority) when VotesAck >= Majority ->
  true;
have_majority(#candidate_state{votes_nack = VotesNack}, Majority) when VotesNack >= Majority ->
  false;
have_majority(_State, _Majority) ->
  not_enough_votes.

send_heartbeat(#state{collaborators = Collaborators, term = Term, leader = Leader}) ->
  Req = #heartbeat_req{leader = Leader, term = Term},
  [gen_server:cast(Pid, Req) || Pid <- Collaborators, Pid =/= self()].

send_change_state(#state{term = Term, collaborators = Collaborators, leader = Leader},
                  TargetCollaborators) ->
  ChangeState = #change_state_req{collaborators = Collaborators, term = Term, leader = Leader},
  [gen_server:cast(Pid, ChangeState) || Pid <- TargetCollaborators, Pid =/= self()].

send_change_state(#state{collaborators = Collaborators} = State) ->
  send_change_state(State, Collaborators).

get_timeout() ->
  Max = application:get_env(raft, max_heartbeat_timeout, ?MAX_HEARTBEAT_TIMEOUT),
  Min = application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT),
  GraceTime = application:get_env(raft, heartbeat_grace_time, ?HEARTBEAT_GRACE_TIME),
  Rand = rand:uniform(Max-Min),
  Min+Rand+GraceTime.

get_min_timeout() ->
  application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT).

link_collaborators(#state{collaborators = Collaborators},
                   #state{collaborators = NewCollaborators}) ->
  LeavingCollaborators = Collaborators -- NewCollaborators,

  lists:foreach(fun erlang:unlink/1, LeavingCollaborators),
  JoiningCollaborators = NewCollaborators -- Collaborators,
  lists:foreach(fun erlang:link/1, JoiningCollaborators),
  lists:foreach(fun(Pid) -> erlang:monitor(process, Pid) end, JoiningCollaborators).

catch_up(#state{leader = undefined} = State) ->
  State;
catch_up(#state{leader = Leader, log = Log} = State) ->
  CatchupReq = #catch_up_req{log_pos = raft_log:get_pos(Log), target = self()},
  case catch gen_statem:call(Leader, CatchupReq) of
    {ok, Ref} ->
      {ok, NewState} = read_log_stream(Ref, State),
      NewState;
    {'EXIT', _} ->
      State;
    {error, _} ->
      State
  end.

read_log_stream(Ref, #state{log = Log} = State) ->
  case raft_log:read_stream(Ref, 35000) of
    {ok, _Pos, Data} ->
      NewLog = raft_log:append(Data, Log),
      {reply, _, NewState} = execute_log(Data, State),
      read_log_stream(Ref, NewState#state{log = NewLog});
    '$end_of_stream' ->
      {ok, State};
    timeout ->
      timeout
  end.


init_user_state(#state{callback = CallbackModule} = State) ->
  State#state{user_state = apply(CallbackModule, init, [])}.

execute_log(Command, #state{callback = CallbackModule, user_state = UserState} = State) ->
  ?LOG("[~p] execute log command ~p~n", [self(), Command]),
  case apply(CallbackModule, execute, [Command, UserState]) of
    {reply, Reply, NewUserState} ->
      {reply, Reply, State#state{user_state = NewUserState}}
  end.

log_append_majority(_LogPos, Target, Ack, _Nack) when Target =< Ack ->
  true;
log_append_majority(_LogPos, Target, _Ack, Nack) when Target =< Nack ->
  false;
log_append_majority(LogPos, Target, Ack, Nack) ->
  receive
    {'$gen_cast', #append_log_ack{log_pos = LogPos, collaborator = _Collaborator}} ->
      ?LOG("[~p] ~p got append_log ACK from ~p~n" ,[self(), LogPos, _Collaborator]),
      log_append_majority(LogPos, Target, Ack+1, Nack);
    {'$gen_cast', #append_log_nack{log_pos = LogPos, collaborator = _Collaborator}} ->
      ?LOG("[~p] ~p got append_log NACK from ~p~n" ,[self(), LogPos, _Collaborator]),
      log_append_majority(LogPos, Target, Ack, Nack+1)
  after application:get_env(raft, consensus_timeout, ?CONSENSUS_TIMEOUT) ->
    ?LOG("[~p] ~p log_append_majdority TIMEOUT ~n" ,[self(), LogPos]),
    false
  end.

get_majority_number(CollaboratorCount) ->
  (CollaboratorCount div 2) + 1.