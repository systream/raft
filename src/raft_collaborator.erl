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
% erlang has monitors/links so do not really need timeouts, just in case.
-define(MAX_HEARTBEAT_TIMEOUT, 35000).
-define(MIN_HEARTBEAT_TIMEOUT, 15000).
-define(HEARTBEAT_GRACE_TIME, 5000).

-define(ELECTION_TIMEOUT(Timeout), {state_timeout, Timeout, election_timeout}).
-define(ELECTION_TIMEOUT, ?ELECTION_TIMEOUT(get_timeout())).

-define(JOIN_MEMBER_CMD, '$raft_join_member').
-define(JOIN_MEMBER_CMD(Member), {?JOIN_MEMBER_CMD, Member}).
-define(LEAVE_MEMBER_CMD, '$raft_leave_member').
-define(LEAVE_MEMBER_CMD(Member), {?LEAVE_MEMBER_CMD, Member}).

%% API
-export([start_link/1, status/1, command/2, join/2, leave/2]).

%% gen_statem callbacks
-export([init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0]).

-define(SERVER, ?MODULE).

-type(state_name() :: follower | candidate | leader).

-include("raft.hrl").

-record(follower_state, {
  %committed_index :: log_index()
}).

-record(candidate_state, {
  majority :: pos_integer(),
  granted = 0 :: non_neg_integer()
}).

-record(leader_state, {
  %committed_index :: log_index()
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
  log :: raft_log:log_ref(),

  leader :: pid() | undefined,

  committed_index :: log_index(),
  last_applied :: log_index(),

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
  entries :: [command()],
  leader_commit_index :: log_index()
}).

-record(append_entries_resp, {
  term :: raft_term(),
  success :: boolean()
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

-spec join(pid(), pid()) -> ok.
join(ActualClusterMember, NewClusterMember) ->
  command(ActualClusterMember, ?JOIN_MEMBER_CMD(NewClusterMember)).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  command(ClusterMember, ?LEAVE_MEMBER_CMD(MemberToLeave)).

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
                                        callback = CallbackModule,
                                        log = raft_log:new()})}.

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
  {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT(0)]};
handle_event(enter, _PrevStateName, follower, State) ->
  ?LOG("[~p, follower] become follower and catch_up~n", [self()]),
  {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT]};
handle_event(state_timeout, election_timeout, follower, State) ->
  ?LOG("[~p, follower] heartbeat timeout~n", [self()]),
  {next_state, candidate, State};

%% %%%%%%%%%%%%%%%
%% Candidate
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, candidate,
             State = #state{current_term = Term, collaborators = Collaborators, log = Log}) ->
  % To begin an election, a follower increments its current term and transitions to candidate state
  ?LOG("[~p, candidate] Become candidate ~n", [self()]),
  NewTerm = Term+1,
  Me = self(),

  VoteReq = #vote_req{
    term = NewTerm,
    candidate = Me,
    last_log_index = raft_log:last_index(Log),
    last_log_term = raft_log:last_term(Log)
  },

  % It then votes for itself and issues RequestVote RPCs in parallel to each of
  % the other servers in the cluster
  erlang:send(Me, #vote_req_reply{term = NewTerm, granted = true}),
  send(Collaborators, VoteReq),
  InnerState = #candidate_state{majority = majority_count(length(Collaborators))},
  NewState = State#state{current_term = NewTerm, inner_state = InnerState,
                         leader = undefined, voted_for = undefined},
  {keep_state, NewState, [?ELECTION_TIMEOUT]};

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
handle_event(state_timeout, election_timeout, candidate, State) ->
  {repeat_state, State};

%% %%%%%%%%%%%%%%%%%
%% All Servers:
%% %%%%%%%%%%%%%%%%%%
% • If commitIndex > lastApplied: increment lastApplied, apply
% log[lastApplied] to state machine (§5.3)
% • If RPC request or response contains term T > currentTerm:
% set currentTerm = T, convert to follower (§5.1)

% Receiver implementation:
% 1. Reply false if term < currentTerm (§5.1)
% 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
% 3. If an existing entry conflicts with a new one (same index
% but different terms), delete the existing entry and all that follow it (§5.3)
% 4. Append any new entries not already in the log
% 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
handle_event(info, #append_entries_req{term = Term, leader = Leader}, _,
             #state{current_term = CurrentTerm}) when Term < CurrentTerm ->
  send_msg(Leader, #append_entries_resp{term = CurrentTerm, success = false}),
  keep_state_and_data;
handle_event(info,
             #append_entries_req{prev_log_index = PrevLogIndex, leader = Leader, term = Term,
                                 leader_commit_index = LeaderCommitIndex} = AppendReq,
             _StateName,
             #state{log = Log, current_term = Term, committed_index = CI} = State) ->
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {_Index, LogTerm, _Command}} when AppendReq#append_entries_req.term =/= LogTerm ->
      send_msg(Leader, #append_entries_resp{term = Term, success = false}),
      maybe_change_term(AppendReq, State);
    _ ->
      NewLog = append_commands(AppendReq, Log),
      send_msg(Leader, #append_entries_resp{term = Term, success = true}),
      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CI, NewLog),
      NewState = State#state{log = NewLog, current_term = Term, leader = Leader,
                             committed_index = NewCommitIndex},
      maybe_change_term(AppendReq, maybe_apply(NewState))
  end;
% handle vote requests
% If votedFor is null or candidateId, and candidate’s log is at
% least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm} = State) when Term < CurrentTerm ->
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false}),
  {keep_state, State};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = undefined} = State) ->
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = true}),
  {keep_state, State#state{voted_for = Candidate}};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = Candidate} = State) ->
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = true}),
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
   [{state_timeout, get_min_timeout(), election_timeout}]};
handle_event(state_timeout, election_timeout, leader, State) ->
  send_heartbeat(State),
  {keep_state_and_data, [{state_timeout, get_min_timeout(), election_timeout}]};
handle_event({call, From}, {command, Command}, leader,
             #state{log = Log, current_term = Term} = State) ->
  ?LOG("[~p, leader] command (~p) ~p~n", [self(), From, Command]),
  NewLog = raft_log:append(Log, Command, Term),

  % send with old state (old log ref) to get last_term/last_index to previous
  send_append_req([Command], State),

  % @TODO: majority commit

  NewState = State#state{log = NewLog},
  {reply, Reply, NewState2} = execute(Command, raft_log:last_index(NewLog), NewState),
  gen_statem:reply(From, Reply),
  {keep_state, NewState2};

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

handle_event(EventType, EventContent, StateName, #state{} = State) ->
  ?LOG("[~p, ~p] unhandled: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n~n",
       [self(), StateName, EventType, EventContent, State, StateName]),
  keep_state_and_data.


%% @private
%% @doc This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
terminate(_Reason, _StateName, #state{log = Log} = _State) ->
  raft_log:destroy(Log),
  ok.

%% @private
%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State = #state{}, _Extra) ->
  {ok, StateName, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
send_heartbeat(State) ->
  send_append_req([], State).

send_append_req(Commands, #state{current_term = Term, leader = Leader,
                                 log = Log, collaborators = Servers,
                                 committed_index = CI}) ->
  AppendEntriesReq = #append_entries_req{term = Term,
                                         leader = Leader,
                                         prev_log_index = raft_log:last_index(Log),
                                         prev_log_term = raft_log:last_term(Log),
                                         entries = Commands,
                                         leader_commit_index = CI},
  send(Servers, AppendEntriesReq).

send(Collaborators, Msg) ->
  Me = self(),
  [send_msg(Collaborator, Msg) || Collaborator <- Collaborators, Collaborator =/= Me].

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

execute(Command, Index, #state{callback = CallbackModule, user_state = UserState} = State) ->
  ?LOG("[~p] execute log command ~p~n", [self(), Command]),
  case apply(CallbackModule, execute, [Command, UserState]) of
    {reply, Reply, NewUserState} ->
      {reply, Reply, State#state{user_state = NewUserState,
                                 committed_index = Index}}
  end.


send_msg(Target, Msg) ->
  erlang:send_nosuspend(Target, Msg).

maybe_change_term(#append_entries_req{term = Term}, #state{current_term = CurrentTerm} = State)
  when Term < CurrentTerm ->
  {next_state, follower, State#state{current_term = Term, voted_for = undefined}};
maybe_change_term(_, State) ->
  {keep_state, State}.

maybe_delete_log(Log, #append_entries_req{prev_log_index = PrevLogIndex, term = AppendTerm}) ->
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {Index, LogTerm, _Command}} when AppendTerm =/= LogTerm ->
      % 3. If an existing entry conflicts with a new one (same index
      % but different terms), delete the existing entry and all that follow it (§5.3)
      raft_log:delete(Log, Index);
    _ ->
      Log
  end.

append_commands(#append_entries_req{entries = Commands, term = Term} = AppendReq, Log) ->
  NewLog = maybe_delete_log(Log, AppendReq),
  lists:foldl(fun(Command, CLog) -> raft_log:append(CLog, Command, Term) end, NewLog, Commands).

% 5. If leaderCommit > commitIndex,
% set commitIndex = min(leaderCommit, index of last new entry)
maybe_update_commit_index(LeaderCI, CI, Log) when LeaderCI > CI ->
  erlang:min(LeaderCI, raft_log:last_index(Log));
maybe_update_commit_index(_LeaderCommitIndex, CommitIndex, _Log) ->
  CommitIndex.

maybe_apply(#state{last_applied = LastApplied, committed_index = CommittedIndex, log = Log} = State)
  when CommittedIndex > LastApplied ->
  {ok, {Index, _Term, Command}} = raft_log:get(Log, LastApplied+1),
  {reply, _, NewState} = execute(Command, Index, State),
  maybe_apply(NewState);
maybe_apply(State) ->
  State.