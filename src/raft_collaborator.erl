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

-define(LOG(Msg, Args), io:format(user, "[~p] " ++ Msg, [erlang:system_time(millisecond) | Args])).
%-define(LOG(_Msg, _Args), ok).

% election timeouts
-define(MAX_HEARTBEAT_TIMEOUT, 35000).
-define(MIN_HEARTBEAT_TIMEOUT, 15000).
-define(HEARTBEAT_GRACE_TIME, 5000).

-define(CONSENSUS_TIMEOUT, 5000).

-define(HEARTBEAT_STATE_TIMEOUT(Timeout), {state_timeout, Timeout, heartbeat_timeout}).
-define(HEARTBEAT_STATE_TIMEOUT, ?HEARTBEAT_STATE_TIMEOUT(get_timeout())).

%% API
-export([start_link/1, status/1, command/2]).

%% gen_statem callbacks
-export([init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0]).

-define(SERVER, ?MODULE).

-type(state_name() :: follower | candidate | leader).

-type(raft_term() :: non_neg_integer()).
-type(command() :: term()).
-type(log_index() :: pos_integer()).

-record(log_entry, {
  log_index :: log_index(),
  term :: raft_term(),
  command :: command()
}).

-type(log_entry() :: #log_entry{}).

-record(follower_state, {

}).

-record(candidate_state, {
  majority :: pos_integer(),
  granted = 0 :: non_neg_integer()
}).

-record(leader_state, {

}).

-type(follower_state() :: #follower_state{}).
-type(candidate_state() :: #candidate_state{}).
-type(leader_state() :: #leader_state{}).

-record(state, {
  callback :: module(),
  user_state :: term(),
  collaborators = [] :: [pid()],

  current_term = 0 :: raft_term(),
  voted_for :: pid() | undefined,
  log = [] :: [log_entry()],

  leader :: pid() | undefined,

  inner_state :: follower_state() | candidate_state() | leader_state() | undefined
}).

-type(state() :: #state{}).

-record(vote_req, {
  term :: raft_term(),
  candidate :: pid(),
  last_log_index :: log_index(),
  last_log_term :: raft_term()
}).

-record(vote_req_reply, {
  term :: raft_term(),
  granted :: boolean()
}).

-record(append_entries_req, {
  term :: raft_term(),
  leader :: pid(),
  prev_log_index :: log_index(),
  prev_log_term :: raft_term(),
  entries :: [log_entry()],
  leader_commit_index :: log_index()
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

-spec init(module()) -> {ok, follower, state()}.
init(CallbackModule) ->
  erlang:process_flag(trap_exit, true),
  ?LOG("[~p] starting ~p~n", [self(), CallbackModule]),
  {ok, follower, init_user_state(#state{collaborators = [self()],
                                        callback = CallbackModule})}.

-spec callback_mode() -> [handle_event_function | state_enter].
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
%% @doc Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
format_status(_Opt, [_PDict, StateName, _State]) ->
  {_Opt, [_PDict, StateName, _State]}.


%% %%%%%%%%%%%%%%%
%% Follower
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, follower, #state{collaborators = [Collaborator]} = State)
  when Collaborator =:= self() ->
  % Alone in cluster, no one will send heartbeat, so step up for candidate immediately
  ?LOG("[~p, follower] start follower~n", [self()]),
  {keep_state, State#state{inner_state = #follower_state{}}, [?HEARTBEAT_STATE_TIMEOUT(0)]};
handle_event(enter, _PrevStateName, follower, State) ->
  ?LOG("[~p, follower] become follower and catch_up~n", [self()]),
  {keep_state, State#state{inner_state = #follower_state{}}, [?HEARTBEAT_STATE_TIMEOUT]};
handle_event(state_timeout, heartbeat_timeout, follower, State) ->
  ?LOG("[~p, follower] heartbeat timeout~n", [self()]),
  {next_state, candidate, State};

%% %%%%%%%%%%%%%%%
%% Candidate
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, candidate,
             State = #state{current_term = Term, collaborators = Collaborators}) ->
  % To begin an election, a follower increments its current term and transitions to candidate state
  ?LOG("[~p, candidate] Become candidate ~n", [self()]),
  NewTerm = Term+1,
  Me = self(),

  VoteReq = #vote_req{
    term = NewTerm,
    candidate = Me %,
  % @TODO
    %last_log_index = LastLogIndex,
    %last_log_term = LastLogTerm
  },

  % It then votes for itself and issues RequestVote RPCs in parallel to each of
  % the other servers in the cluster
  erlang:send(Me, #vote_req_reply{term = NewTerm, granted = true}),
  send(Collaborators, VoteReq),
  InnerState = #candidate_state{majority = majority_count(length(Collaborators))},
  NewState = State#state{current_term = NewTerm, inner_state = InnerState,
                         leader = undefined, voted_for = undefined},
  {keep_state, NewState, [?HEARTBEAT_STATE_TIMEOUT]};

% process vote reply
handle_event(info, #vote_req_reply{term = Term, granted = true}, candidate,
             #state{current_term = Term,
                    inner_state = InnerState = #candidate_state{granted = Granted}} = State) ->
  NewInnerState = InnerState#candidate_state{granted = Granted+1},
  case NewInnerState of
    #candidate_state{granted = G, majority = M} when G < M ->
      {keep_state, State#state{inner_state = NewInnerState}};
    _ ->
      {next_state, leader, State#state{inner_state = NewInnerState}}
  end;
handle_event(info, #vote_req_reply{term = Term, granted = false}, candidate,
             State = #state{current_term = CurrentTerm}) when Term > CurrentTerm ->
  {keep_state, State = #state{current_term = Term, voted_for = undefined}};
handle_event(info, #vote_req_reply{granted = false}, candidate, State) ->
  {keep_state, State};
handle_event(state_timeout, heartbeat_timeout, candidate, State) ->
  {repeat_state, State#state};

%% %%%%%%%%%%%%%%%%%
%% All Servers:
%% %%%%%%%%%%%%%%%%%%
% • If commitIndex > lastApplied: increment lastApplied, apply
% log[lastApplied] to state machine (§5.3)
% • If RPC request or response contains term T > currentTerm:
% set currentTerm = T, convert to follower (§5.1)
handle_event(info, #append_entries_req{term = Term}, _,
             #state{current_term = CurrentTerm} = State) when Term > CurrentTerm ->
  % @todo first bulletpoint
  {next_state, follower, State#state{current_term = Term, voted_for = undefined}};
% handle vote requests
% If votedFor is null or candidateId, and candidate’s log is at
% least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm} = State) when Term < CurrentTerm ->
  erlang:send_nosuspend(Candidate, #vote_req_reply{term = CurrentTerm, granted = false}),
  {keep_state, State};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = undefined} = State) ->
  erlang:send_nosuspend(Candidate, #vote_req_reply{term = CurrentTerm, granted = true}),
  {keep_state, State#state{voted_for = Candidate}};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = Candidate} = State) ->
  erlang:send_nosuspend(Candidate, #vote_req_reply{term = CurrentTerm, granted = true}),
  {keep_state, State};

%% %%%%%%%%%%%%%%%
%% Leader
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, leader, State) ->
  ?LOG("[~p, leader] become leader~n", [self()]),
  % Upon election: send initial empty AppendEntries RPCs
  % (heartbeat) to each server; repeat during idle periods to
  % prevent election timeouts (§5.2)
  send_heartbeat(State),
  {keep_state, State#state{leader = self(), inner_state = #leader_state{}},
   [{state_timeout, get_min_timeout(), heartbeat_timeout}]};
handle_event(state_timeout, heartbeat_timeout, leader, State) ->
  send_heartbeat(State),
  {keep_state_and_data, [{state_timeout, get_min_timeout(), heartbeat_timeout}]};
handle_event({call, From}, {command, Command}, leader, _State) ->
  ?LOG("[~p, leader] command (~p) ~p~n", [self(), From, Command]),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% Proxy commands to leader
%% %%%%%%%%%%%%%%%%%%%%%%%%%

% handle proxy request
handle_event(info, {proxy, Type, Context}, leader, State) ->
  ?LOG("[~p, leader] got proxy ~p~n", [self(), {proxy, Type, Context}]),
  handle_event(Type, Context, leader, State);

% no leader, postpone request for later processing
handle_event(info, {proxy, _Type, _Context}, _StateName, #state{leader = undefined}) ->
  {keep_state_and_data, [postpone]};

% now we have leader, but unfortunately we need to forward it (again)
handle_event(info, {proxy, Type, Context}, _StateName, #state{leader = Leader}) ->
  gen_statem:cast(Leader, {proxy, Type, Context}),
  keep_state_and_data;

handle_event(EventType, EventContent, StateName, #state = State) ->
  ?LOG("[~p, ~p] unhandled: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n~n",
       [self(), StateName, EventType, EventContent, State, StateName]),
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
send_heartbeat(#state{collaborators = Collaborators, current_term = Term, leader = Leader}) ->
  AppendEntriesReq = #append_entries_req{term = Term,
                                         leader = Leader,
                                         prev_log_index = 1,
                                         prev_log_term = 1,
                                         entries = [],
                                         leader_commit_index = 1},
  send(Collaborators, AppendEntriesReq).

send(Collaborators, Msg) ->
  Me = self(),
  [erlang:send_nosuspend(Collaborator, Msg) || Collaborator <- Collaborators, Collaborator =/= Me].

get_timeout() ->
  Max = application:get_env(raft, max_heartbeat_timeout, ?MAX_HEARTBEAT_TIMEOUT),
  Min = application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT),
  GraceTime = application:get_env(raft, heartbeat_grace_time, ?HEARTBEAT_GRACE_TIME),
  Rand = rand:uniform(Max-Min),
  Min+Rand+GraceTime.

get_min_timeout() ->
  application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT).

init_user_state(#state{callback = CallbackModule} = State) ->
  State#state{user_state = apply(CallbackModule, init, [])}.

majority_count(CollaboratorCount) ->
  (CollaboratorCount div 2) + 1.