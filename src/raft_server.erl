%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(raft_server).
-author("Peter Tihanyi").

-behaviour(gen_statem).

-include("raft.hrl").

% election timeouts
% erlang has monitors/links so do not really need timeouts, just in case.
-define(MAX_HEARTBEAT_TIMEOUT, 10000).
-define(MIN_HEARTBEAT_TIMEOUT, 250).
-define(HEARTBEAT_GRACE_TIME, 150).

-define(ELECTION_TIMEOUT(Timeout), {state_timeout, Timeout, election_timeout}).
-define(ELECTION_TIMEOUT, ?ELECTION_TIMEOUT(get_timeout())).

-define(CLUSTER_CHANGE_COMMAND, '$raft_cluster_change_command').
-define(CLUSTER_CHANGE, '$raft_cluster_change').
-define(JOINT_CLUSTER_CHANGE, '$raft_joint_cluster_change').

%% API
-export([start_link/1, stop/1,
         status/1,
         command/2,
         join/2, leave/2]).

%% gen_statem callbacks
-export([init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0]).

-define(SERVER, ?MODULE).

%-type(state_name() :: follower | candidate | leader).

-record(follower_state, {
  %committed_index :: log_index()
}).

-record(candidate_state, {
  granted = 0 :: non_neg_integer()
}).

-record(active_request, {
  log_index :: log_index(),
  from :: gen_statem:from(),
  replicated = [] :: [pid()]
}).

-type(active_request() :: #active_request{}).

-record(leader_state, {
    peers = #{} :: #{pid() => raft_peer:peer()},
    active_requests = #{} :: #{log_index() => active_request()}
}).

-type(follower_state() :: #follower_state{}).
-type(candidate_state() :: #candidate_state{}).
-type(leader_state() :: #leader_state{}).

-record(state, {
  callback :: module(),
  user_state :: term(),
  cluster :: raft_cluster:cluster(),

  current_term = 0 :: raft_term(),
  voted_for :: pid() | undefined,
  log :: raft_log:log_ref(),

  committed_index = 0 :: log_index(),
  last_applied = 0 :: log_index(),

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
  voter :: pid(),
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
  server :: pid(),
  match_index :: log_index() | undefined,
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

-spec stop(pid()) -> ok.
stop(Server) ->
  gen_statem:stop(Server).

-spec status(pid()) -> map().
status(ClusterMember) ->
  {status, _Pid, {module, _Module}, [_, _, _, _, SItems]} = sys:get_status(ClusterMember, 30000),
  proplists:get_value(state, SItems).

-spec join(pid(), pid()) -> ok.
join(ClusterMember, NewClusterMember) ->
  gen_statem:call(ClusterMember, {join, NewClusterMember}, 30000).

-spec leave(pid(), pid()) -> ok.
leave(ClusterMember, MemberToLeave) ->
  gen_statem:call(ClusterMember, {leave, MemberToLeave}, 30000).

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
  logger:info("Starting raft server with callback: ~p", [CallbackModule]),
  {ok, follower, init_user_state(#state{callback = CallbackModule,
                                        log = raft_log:new(),
                                        cluster = raft_cluster:new()})}.

-spec callback_mode() -> [handle_event_function | state_enter].
callback_mode() ->
  [handle_event_function, state_enter].

%% @private
%% @doc Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
format_status(_Opt, [_PDict, StateName, #state{current_term = CurrentTerm, cluster = Cluster,
                                               committed_index = Ci, log = Log,
                                               last_applied = La}]) ->
  LastIndex = raft_log:last_index(Log),
  {state, #{role => StateName,
    term => CurrentTerm,
    leader => raft_cluster:leader(Cluster),
    cluster_members => raft_cluster:members(Cluster),
    log_last_index => LastIndex,
    committed_index => Ci,
    last_applied => La,
    log =>
    lists:reverse([begin {ok, {Term, Command}} = raft_log:get(Log, Idx), {Idx, Term, Command} end ||
                   Idx <- lists:seq(1, LastIndex)])
   }}.

%% %%%%%%%%%%%%%%%
%% Follower
%% %%%%%%%%%%%%%%%
handle_event(enter, PrevStateName, follower, #state{cluster = Cluster} = State) ->
  logger:info("Became a follower", []),
  set_logger_meta(#{raft_role => follower}),
  case PrevStateName of
    leader ->
      cancel_all_pending_requests(State#state.inner_state);
    _ ->
      ok
  end,
  case raft_cluster:majority_count(Cluster) of
    1 ->
      % Alone in cluster, no one will send heartbeat, so step up for candidate immediately
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT(100)]};
    _ ->
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT]}
  end;
handle_event(state_timeout, election_timeout, follower, State) ->
  logger:notice("Heartbeat timeout", []),
  {next_state, candidate, State};

%% %%%%%%%%%%%%%%%
%% Candidate
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, candidate,
             State = #state{current_term = Term, cluster = Cluster, log = Log}) ->

  set_logger_meta(#{raft_role => candidate}),
  % To begin an election, a follower increments its current term and transitions to candidate state
  NewTerm = Term+1,
  logger:info("Became a candidate with term ~p", [NewTerm]),
  Me = self(),

  VoteReq = #vote_req{
    term = NewTerm,
    candidate = Me,
    last_log_index = raft_log:last_index(Log),
    last_log_term = raft_log:last_term(Log)
  },

  % It then votes for itself and issues RequestVote RPCs in parallel to each of
  % the other servers in the cluster
  send_msg(Me, #vote_req_reply{term = NewTerm, voter = self(), granted = true}),
  logger:debug("Send vote request for term ~p", [NewTerm]),

  [send_msg(Server, VoteReq) || Server <- raft_cluster:members(Cluster), Server =/= Me],

  NewState = State#state{current_term = NewTerm, inner_state = #candidate_state{},
                         cluster = raft_cluster:leader(Cluster, undefined),
                         voted_for = self()},
  {keep_state, NewState, [?ELECTION_TIMEOUT(get_consensus_timeout())]};

% process vote reply
handle_event(info, #vote_req_reply{term = Term, voter = Voter, granted = true}, candidate,
             #state{current_term = Term,
                    cluster = Cluster,
                    inner_state = InnerState = #candidate_state{granted = Granted}} = State) ->
  NewInnerState = InnerState#candidate_state{granted = Granted+1},
  logger:debug("Got positive vote reply from ~p for term ~p", [Voter, Term]),
  Majority = raft_cluster:majority_count(Cluster),
  case NewInnerState of
    #candidate_state{granted = NewGranted} when NewGranted < Majority ->
      {keep_state, State#state{inner_state = NewInnerState}};
    _ ->
      logger:info("Have the majority ~p of ~p of the votes for ~p term",
                  [Granted+1, Majority, Term]),
      {next_state, leader, State#state{inner_state = NewInnerState}}
  end;
handle_event(info, #vote_req_reply{term = Term, voter = Voter, granted = false}, candidate,
             #state{current_term = CurrentTerm} = State) when Term > CurrentTerm ->
  logger:notice("Vote request reply from ~p contains higher term Term", [Voter, Term]),
  {next_state, follower, State#state{current_term = Term, voted_for = undefined}};
handle_event(info, #vote_req_reply{term = Term, voter = Voter}, candidate,
             #state{current_term = CurrentTerm} = State) when Term < CurrentTerm ->
  logger:notice("Vote request from ~p ignored", [Voter]),
  {keep_state, State};
handle_event(state_timeout, election_timeout, candidate, State) ->
  logger:notice("Election timeout", []),
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

% append request with lower term
handle_event(info, #append_entries_req{term = Term, leader = Leader}, _StateName,
             #state{current_term = CurrentTerm}) when Term < CurrentTerm ->
  logger:notice("Got append_entries_req with lower Term ~p < ~p, rejecting", [Term, CurrentTerm]),
  send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                        server = self(),
                                        success = false,
                                        match_index = 0}),
  keep_state_and_data;

% append request with higher term
handle_event(info, #append_entries_req{term = Term,
                                       leader = Leader,
                                       entries = Entries,
                                       prev_log_index = PrevLogIndex,
                                       prev_log_term = PrevLogTerm,
                                       leader_commit_index = LeaderCommitIndex}, _StateName,
             #state{current_term = CurrentTerm, committed_index = CommittedIndex,
                    log = Log, cluster = Cluster} = State)
  when Term > CurrentTerm ->
  logger:debug("Got append_entries_req with higher Term ~p > ~p", [Term, CurrentTerm]),
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {PrevLogTerm, _Command}} -> % log exists and the term matched
      NewLog = maybe_delete_log(Log, PrevLogIndex+1, Term),
      NewLog2 = append_commands(NewLog, Entries),
      logger:debug("AppendEntries successfully commited", []),
      send_msg(Leader, #append_entries_resp{term = Term,
                                            server = self(),
                                            success = true,
                                            match_index = raft_log:last_index(NewLog2)}),
      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CommittedIndex, NewLog2),
      NewState = State#state{current_term = Term,
                             voted_for = undefined,
                             log = NewLog2,
                             cluster = raft_cluster:leader(Cluster, Leader),
                             committed_index = NewCommitIndex},
      {next_state, follower, maybe_apply_cluster_changes(Entries, maybe_apply(NewState))};
    _ when PrevLogIndex =:= 0 -> % nothing in the log yet
      NewLog = append_commands(maybe_delete_log(Log, 1, Term), Entries),
      logger:debug("AppendEntries successfully commited", []),
      send_msg(Leader, #append_entries_resp{term = Term,
                                            server = self(),
                                            success = true,
                                            match_index = raft_log:last_index(NewLog)}),
      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CommittedIndex, NewLog),
      NewState = State#state{current_term = Term,
                             voted_for = undefined,
                             log = NewLog,
                             cluster = raft_cluster:leader(Cluster, Leader),
                             committed_index = NewCommitIndex},
      {next_state, follower, maybe_apply_cluster_changes(Entries, maybe_apply(NewState))};
    _  = R->
      logger:notice("Previous log entry ~p not matched reply false -> ~p", [PrevLogIndex, R]),
      % reply false, and step down
      send_msg(Leader, #append_entries_resp{term = Term,
                                            server = self(),
                                            success = false,
                                            match_index = raft_log:last_index(Log)}),
      {next_state, follower, State#state{current_term = Term,
                                         voted_for = undefined,
                                         cluster = raft_cluster:leader(Cluster, Leader)}}
  end;

% append entries with same term
handle_event(info, #append_entries_req{term = Term,
                                       leader = Leader,
                                       entries = Entries,
                                       prev_log_index = PrevLogIndex,
                                       prev_log_term = PrevLogTerm,
                                       leader_commit_index = LeaderCommitIndex}, StateName,
  #state{current_term = Term, committed_index = CommittedIndex,
         log = Log,
         cluster = Cluster} = State) ->
  logger:debug("Got append_entries_req with ~p term, prev_log_index ~p", [Term, PrevLogIndex]),
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {PrevLogTerm, _Command}} -> % log exists and the term matched
      NewLog = maybe_delete_log(Log, PrevLogIndex+1, Term),
      logger:debug("log pos: ~p", [raft_log:last_index(NewLog)]),
      NewLog2 = append_commands(NewLog, Entries),
      logger:debug("AppendEntries successfully commited", []),
      send_msg(Leader, #append_entries_resp{term = Term,
                                            server = self(),
                                            success = true,
                                            match_index = raft_log:last_index(NewLog2)}),
      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CommittedIndex, NewLog2),
      NewState = State#state{current_term = Term,
                             voted_for = undefined,
                             log = NewLog2,
                             cluster = raft_cluster:leader(Cluster, Leader),
                             committed_index = NewCommitIndex},
      NewState2 = maybe_apply_cluster_changes(Entries, maybe_apply(NewState)),
      case StateName of
        leader ->
          {next_state, follower, NewState2};
        _ ->
          {keep_state, NewState2, [?ELECTION_TIMEOUT]}
      end;
    _ when PrevLogIndex =:= 0 -> % nothing in the log yet
      NewLog = maybe_delete_log(Log, 1, Term),
      logger:debug("log pos: ~p", [raft_log:last_index(NewLog)]),
      NewLog2 = append_commands(NewLog, Entries),
      logger:debug("AppendEntries successfully commited", []),
      send_msg(Leader, #append_entries_resp{term = Term,
                                            server = self(),
                                            success = true,
                                            match_index = raft_log:last_index(NewLog2)}),
      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CommittedIndex, NewLog2),
      NewState = State#state{current_term = Term,
                             voted_for = undefined,
                             log = NewLog2,
                             cluster = raft_cluster:leader(Cluster, Leader),
                             committed_index = NewCommitIndex},
      NewState2 = maybe_apply_cluster_changes(Entries, maybe_apply(NewState)),
      case StateName of
        leader ->
          {next_state, follower, NewState2};
        _ ->
          {keep_state, NewState2, [?ELECTION_TIMEOUT]}
      end;
    _ = R ->
      logger:notice("Previous log entry ~p not matched reply false -> ~p", [PrevLogIndex, R]),
      % reply false, and step down
      send_msg(Leader, #append_entries_resp{term = Term,
                                            server = self(),
                                            success = false,
                                            match_index = raft_log:last_index(Log)}),
      {keep_state, State#state{current_term = Term, voted_for = undefined}}
  end;
% ************************
% handle vote requests
% ************************
% If votedFor is null or candidateId, and candidate’s log is at
% least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

% Candidate has lower term
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm} = State) when Term < CurrentTerm ->
  logger:notice("Got vote_req for ~p with lower term ~p < ~p, rejecting",
                 [Candidate, Term, CurrentTerm]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, voter = self(), granted = false}),
  {keep_state, State};

% Candidate has higher term, convert to follower
handle_event(info,
             #vote_req{term = Term,
                       candidate = Candidate,
                       last_log_term = CandidateLastLogTerm,
                       last_log_index = CandidateLastLogIndex},
             _StateName,
             #state{current_term = CurrentTerm,
                    cluster = Cluster,
                    log = Log} = State) when Term > CurrentTerm ->

  LastLogTerm = raft_log:last_term(Log),
  LastLogIndex = raft_log:last_index(Log),
  case LastLogTerm > CandidateLastLogTerm of
    true ->
      logger:notice("Got vote_req for ~p with lower log term ~p > ~p, rejecting",
                    [Candidate, LastLogTerm, CandidateLastLogTerm]),
      send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = false}),
      {next_state, follower, State#state{current_term = Term,
                                         cluster = raft_cluster:leader(Cluster, undefined),
                                         voted_for = undefined}};
    false when LastLogTerm =:= CandidateLastLogTerm andalso LastLogIndex > CandidateLastLogIndex ->
      logger:notice("Got vote_req for ~p with lower log index ~p > ~p, rejecting",
                    [Candidate, LastLogIndex, CandidateLastLogIndex]),
      send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = false}),
      {next_state, follower, State#state{current_term = Term,
                                         cluster = raft_cluster:leader(Cluster, undefined),
                                         voted_for = undefined}};
    _ ->
      logger:notice("Got vote_req for ~p with higher term ~p > ~p, accepting",
                    [Candidate, Term, CurrentTerm]),
      send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = true}),
      {next_state, follower, State#state{current_term = Term,
                                         cluster = raft_cluster:leader(Cluster, undefined),
                                         voted_for = Candidate}}
  end;

% Candidate has the same term, not voted yet
handle_event(info,
             #vote_req{term = Term, candidate = Candidate,
                       last_log_index = CandidateLastLogIndex,
                       last_log_term = CandidateLastLogTerm}, _StateName,
             #state{current_term = Term, log = Log, voted_for = undefined} = State) ->
  LastLogTerm = raft_log:last_term(Log),
  LastLogIndex = raft_log:last_index(Log),
  case LastLogTerm > CandidateLastLogTerm of
    true ->
      logger:notice("Got vote_req for ~p with lower log term ~p > ~p, rejecting",
                    [Candidate, LastLogTerm, CandidateLastLogTerm]),
      send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = false}),
      {keep_state, State};
    false when LastLogTerm =:= CandidateLastLogTerm andalso LastLogIndex > CandidateLastLogIndex ->
      logger:notice("Got vote_req for ~p with lower log index ~p > ~p, rejecting",
                    [Candidate, LastLogIndex, CandidateLastLogIndex]),
      send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = false}),
      {keep_state, State};
    _ ->
      logger:notice("Got vote_req for ~p with higher term ~p > ~p, accepting",
                    [Candidate, Term, Term]),
      send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = true}),
      {keep_state, State#state{voted_for = Candidate}, [?ELECTION_TIMEOUT]}
  end;
% Candidate has the same term, and voted for this candidate
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
                   #state{current_term = Term, voted_for = Candidate} = State) ->
  logger:notice("Got vote_req for ~p has already voted for this candidate in this term, accepting",
                [Candidate]),
  send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = true}),
  {keep_state, State, [?ELECTION_TIMEOUT]};
% Candidate has the same term, voted for someone else
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = Term, voted_for = VotedFor} = State) ->
  logger:notice("Got vote_req for ~p has already voted for ~p in this term, rejecting",
                [Candidate, VotedFor]),
  send_msg(Candidate, #vote_req_reply{term = Term, voter = self(), granted = false}),
  {keep_state, State};

%% %%%%%%%%%%%%%%%
%% Leader
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, leader, #state{cluster = Cluster, log = Log} = State) ->
  logger:info("Become leader", []),
  set_logger_meta(#{raft_role => leader}),
  NewCluster = raft_cluster:leader(Cluster, self()),
  NewState = State#state{cluster = NewCluster,
                         inner_state = #leader_state{peers = update_peers(#{}, NewCluster, Log)}},

  % Upon election: send initial empty AppendEntries RPCs
  % (heartbeat) to each server; repeat during idle periods to
  % prevent election timeouts (§5.2)
  send_heartbeat(NewState),
  {keep_state, NewState, [?ELECTION_TIMEOUT(get_min_timeout())]};
handle_event(state_timeout, election_timeout, leader, State) ->
  send_heartbeat(State),
  {keep_state_and_data, [?ELECTION_TIMEOUT(get_min_timeout())]};

handle_event({call, From}, {command, Command}, leader, State) ->
  {keep_state, handle_command(From, Command, State), [?ELECTION_TIMEOUT(get_min_timeout())]};
handle_event({call, From}, {join, Server}, leader,
             #state{cluster = Cluster} = State) ->
  case raft_cluster:join(Server, Cluster) of
    {ok, NewCluster} ->
      {keep_state,
       handle_command(From, {?CLUSTER_CHANGE_COMMAND, NewCluster}, State),
       [?ELECTION_TIMEOUT(get_min_timeout())]};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;
handle_event({call, From}, {leave, Server}, leader,
             #state{cluster = Cluster} = State) ->
  case raft_cluster:leave(Server, Cluster) of
    {ok, NewCluster} ->
      {keep_state,
       handle_command(From, {?CLUSTER_CHANGE_COMMAND, NewCluster}, State),
       [?ELECTION_TIMEOUT(get_min_timeout())]};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;
handle_event(info, #append_entries_resp{term = Term}, _StateName,
        #state{current_term = CurrentTerm, cluster = Cluster} = State)
  when CurrentTerm < Term ->
  logger:notice("Got append_entries_response with higher term ~p, converting to follower", [Term]),
  NewState = State#state{current_term = Term,
                         voted_for = undefined,
                         cluster = raft_cluster:leader(Cluster, undefined)},
  {next_state, follower, NewState};
handle_event(info, #append_entries_resp{term = Term}, _StateName,
             #state{current_term = CurrentTerm} = State)
  when CurrentTerm > Term ->
  logger:notice("Got append_entries_response with lower term ~p, ignoring", [Term]),
  {keep_state, State};
handle_event(info, #append_entries_resp{} = AppendEntriesReq, leader, State) ->
    {keep_state, handle_append_entries_resp(AppendEntriesReq, State)};

%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% Proxy commands to leader
%% %%%%%%%%%%%%%%%%%%%%%%%%%

handle_event({call, From}, {command, Command}, _StateName,
             #state{cluster = Cluster}) ->
  case raft_cluster:leader(Cluster) of
    undefined ->
      logger:notice("Postpone command, no leader yet", []),
      {keep_state_and_data, [postpone]};
    Leader ->
      logger:debug("Proxy command to ~p", [Leader]),
      send_msg(Leader, {proxy, {call, From}, {command, Command}}),
      keep_state_and_data
  end;

handle_event({call, From}, {Type, Node}, _StateName,
             #state{cluster = Cluster}) when Type =:= join orelse Type =:= leave ->
  case raft_cluster:leader(Cluster) of
    undefined ->
      logger:notice("Decline cluster change command, no leader", []),
      gen_statem:reply(From, {error, no_leader}),
      keep_state_and_data;
    Leader ->
      logger:debug("Proxy cluster change command to ~p", [Leader]),
      send_msg(Leader, {proxy, {call, From}, {Type, Node}}),
      keep_state_and_data
  end;

% handle proxy request
handle_event(info, {proxy, Type, Context}, leader, State) ->
  logger:debug("Got proxy request: ~p ~p", [Type, Context]),
  handle_event(Type, Context, leader, State);


handle_event(info, {proxy, Type, Context}, _StateName, #state{cluster = Cluster}) ->
  case raft_cluster:leader(Cluster) of
    undefined ->
      % no leader, postpone request for later processing
      {keep_state_and_data, [postpone]};
    Leader ->
      % now we have leader, but unfortunately we need to forward it (again)
      send_msg(Leader, {proxy, Type, Context}),
      keep_state_and_data
  end;

% not leader, can be dropped?
handle_event(info, #append_entries_resp{}, _StateName, _State) ->
  logger:warning("Got unhandled append entries response, dropped"),
  keep_state_and_data;
handle_event(info, #vote_req_reply{} = Vrp, _StateName, _State) ->
  logger:warning("Got unhandled vote_req_reply, dropped ~p ", [Vrp]),
  keep_state_and_data;

handle_event(info, {'EXIT', Server, _Reason}, _StateName, #state{cluster = Cluster} = State) ->
  case lists:member(Server, raft_cluster:members(Cluster)) of
    true ->
      % if the leader died, start new consensus
      case Server =:= raft_cluster:leader(Cluster) of
        true ->
          logger:error("Cluster leader ~p died", [Server]),
          % all servers might got this message at same time, to give more chance for a success
          % leader election for the first time wait random
          % @TODO find better solution
          timer:sleep(rand:uniform(30)),
          {next_state, candidate, State#state{cluster = raft_cluster:leader(Cluster, undefined)}};
        _ ->
          logger:error("Cluster member ~p died", [Server]),
          keep_state_and_data
      end;
    _ ->
      keep_state_and_data
  end;
handle_event(EventType, EventContent, StateName, #state{} = State) ->
  logger:warning("Unhandled event: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n",
                 [StateName, EventType, EventContent, State, StateName]),
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
  print_record(State),
  % @TODO remove log
  logger:info("sending heartbeat"),
  send_append_reqs(State).

send_append_reqs(#state{log = Log, current_term = Term, committed_index = CommittedIndex,
                        inner_state = #leader_state{peers = Peers}}) ->
  send_append_reqs(Peers, Log, Term, CommittedIndex),
  ok.

get_timeout() ->
  Max = application:get_env(raft, max_heartbeat_timeout, ?MAX_HEARTBEAT_TIMEOUT),
  %Min = application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT),
  GraceTime = application:get_env(raft, heartbeat_grace_time, ?HEARTBEAT_GRACE_TIME),
  Rand = rand:uniform(GraceTime),
  Max-Rand+GraceTime.

get_consensus_timeout() ->
  Min = application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT),
  GraceTime = application:get_env(raft, heartbeat_grace_time, ?HEARTBEAT_GRACE_TIME),
  Rand = rand:uniform(GraceTime),
  Min+Rand.

get_min_timeout() ->
  application:get_env(raft, min_heartbeat_timeout, ?MIN_HEARTBEAT_TIMEOUT) + rand:uniform(5).

init_user_state(#state{callback = CallbackModule} = State) ->
  State#state{user_state = apply(CallbackModule, init, [])}.

execute({?CLUSTER_CHANGE, _NewCluster}, State) ->
  {reply, ok, State};
execute({?JOINT_CLUSTER_CHANGE, {_JointCluster, _FinalCluster}}, State) ->
  {reply, ok, State};
execute(Command, #state{callback = CallbackModule, user_state = UserState} = State) ->
  logger:debug("Execute log command ~p", [Command]),
  case apply(CallbackModule, execute, [Command, UserState]) of
    {reply, Reply, NewUserState} ->
      {reply, Reply, State#state{user_state = NewUserState}}
  end.

apply_cluster_change(NewCluster, State) ->
  NewClusterMembers = raft_cluster:members(NewCluster),
  logger:info("Appling new cluster config ~p", [NewCluster]),
  % Unlink removed node?! or just ignore them when they die?
  case lists:member(self(), NewClusterMembers) of
    true ->
      [link(C) || C <- NewClusterMembers, C =/= self()],
      remove_peers_not_in_cluster(State#state{cluster = NewCluster});
    false ->
      % leaving the cluster, alone in new cluster :(
      %@TODO should stop or not?
      State#state{cluster = raft_cluster:new()}
  end.

remove_peers_not_in_cluster(#state{inner_state = #leader_state{peers = Peers} = LeaderState,
                                   cluster = Cluster} = State) ->
  ClusterMembers = raft_cluster:members(Cluster),
  NewPeers = maps:fold(fun(Key, Peer, Acc) ->
                          case lists:member(raft_peer:server(Peer), ClusterMembers) of
                            true ->
                              Acc#{Key => Peer};
                            false ->
                              Acc
                          end
                        end, #{}, Peers),
  State#state{inner_state = LeaderState#leader_state{peers = NewPeers}};
remove_peers_not_in_cluster(State) ->
  State.

send_msg(Target, Msg) ->
  erlang:send_nosuspend(Target, Msg).

maybe_delete_log(Log, Index, Term) ->
  case raft_log:get(Log, Index) of
    {ok, {LogTerm, _Command}} when Term =/= LogTerm ->
      % 3. If an existing entry conflicts with a new one (same index
      % but different terms), delete the existing entry and all that follow it (§5.3)
      raft_log:delete(Log, Index);
    _ ->
      Log
  end.

append_commands(Log, Commands) ->
  lists:foldl(fun({Term, Command}, CLog) ->
                raft_log:append(CLog, Command, Term)
              end, Log, Commands).

% 5. If leaderCommit > commitIndex,
% set commitIndex = min(leaderCommit, index of last new entry)
maybe_update_commit_index(LeaderCI, CI, Log) when LeaderCI > CI ->
  erlang:min(LeaderCI, raft_log:last_index(Log));
maybe_update_commit_index(_LeaderCommitIndex, CommitIndex, _Log) ->
  CommitIndex.

maybe_apply(#state{last_applied = LastApplied, committed_index = CommittedIndex, log = Log} = State)
  when CommittedIndex > LastApplied ->
  CurrentIndex = LastApplied+1,
  case raft_log:get(Log, CurrentIndex) of
    {ok, {_Term, {?CLUSTER_CHANGE, NewCluster}}} ->
      maybe_apply(State#state{last_applied = CurrentIndex, cluster = NewCluster});
    {ok, {_Term, {?JOINT_CLUSTER_CHANGE, {JointCluster, _FinalCluster}}}} ->
      maybe_apply(State#state{last_applied = CurrentIndex, cluster = JointCluster});
    {ok, {_Term, Command}} ->
      {reply, _, NewState} = execute(Command, State),
      maybe_apply(NewState#state{last_applied = CurrentIndex})
  end;
maybe_apply(State) ->
  State.

handle_command(From, {?CLUSTER_CHANGE_COMMAND, NewCluster} = Command,
               #state{log = Log,
                      cluster = CurrentCluster,
                      current_term = Term,
                      inner_state = #leader_state{active_requests = CurrentRequests,
                                                  peers = Peers} = LeaderState
               } = State) ->
  logger:debug("Handling cluster change: ~p", [Command]),
  JointCluster = raft_cluster:joint_cluster(CurrentCluster, NewCluster),
  JointConsensusClusterChangeCommand = {?JOINT_CLUSTER_CHANGE, {JointCluster, NewCluster}},
  NewLog = raft_log:append(Log, JointConsensusClusterChangeCommand, Term),
  NewLeaderState = LeaderState#leader_state{peers = update_peers(Peers, JointCluster, NewLog)},
  NewState = State#state{log = NewLog, inner_state = NewLeaderState, cluster = JointCluster},
  send_append_reqs(NewState),

  LogIndex = raft_log:last_index(NewLog),
  ActiveRequest = #active_request{log_index = LogIndex, from = From},
  NewCurrentActiveRequests = CurrentRequests#{LogIndex => ActiveRequest},
  NewLeaderState2 = NewLeaderState#leader_state{active_requests = NewCurrentActiveRequests},

  SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                               server = self(),
                                               match_index = LogIndex,
                                               success = true},
  handle_append_entries_resp(SelfAppendEntriesResp, NewState#state{inner_state = NewLeaderState2});
handle_command(From, {?CLUSTER_CHANGE, NewCluster} = Command,
               #state{log = Log,
                      current_term = Term,
                      inner_state = #leader_state{active_requests = CurrentRequests,
                                                  peers = Peers} = LeaderState
               } = State) ->
  logger:debug("Handling cluster change: ~p", [Command]),
  NewLog = raft_log:append(Log, {?CLUSTER_CHANGE, NewCluster}, Term),
  NewLeaderState = LeaderState#leader_state{peers = update_peers(Peers, NewCluster, NewLog)},
  NewState = State#state{log = NewLog, inner_state = NewLeaderState, cluster = NewCluster},
  send_append_reqs(NewState),

  LogIndex = raft_log:last_index(NewLog),
  ActiveRequest = #active_request{log_index = LogIndex, from = From},
  NewCurrentActiveRequests = CurrentRequests#{LogIndex => ActiveRequest},
  NewLeaderState2 = NewLeaderState#leader_state{active_requests = NewCurrentActiveRequests},

  SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                               server = self(),
                                               match_index = LogIndex,
                                               success = true},
  handle_append_entries_resp(SelfAppendEntriesResp, NewState#state{inner_state = NewLeaderState2});
handle_command(From, Command,
               #state{log = Log,
                      current_term = Term,
                      inner_state = #leader_state{active_requests = CurrentRequests} = LeaderState
               } = State) ->
  logger:debug("Handling command: ~p", [Command]),
  print_record(State),

  NewLog = raft_log:append(Log, Command, Term),
  NewState = State#state{log = NewLog},
  send_append_reqs(NewState),

  LogIndex = raft_log:last_index(NewLog),
  ActiveRequest = #active_request{log_index = LogIndex, from = From},
  NewCurrentActiveRequests = CurrentRequests#{LogIndex => ActiveRequest},
  NewLeaderState = LeaderState#leader_state{active_requests = NewCurrentActiveRequests},

  SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                               server = self(),
                                               match_index = LogIndex,
                                               success = true},
  handle_append_entries_resp(SelfAppendEntriesResp, NewState#state{inner_state = NewLeaderState}).


handle_append_entries_resp(#append_entries_resp{success = true,
                                                server = Server,
                                                match_index = MatchIndex} = AppendEntriesResp,
                           #state{inner_state = #leader_state{active_requests = ActiveRequests} = InnerState,
                            cluster = Cluster
                     } = State) ->
  print_record(AppendEntriesResp),
  logger:debug("Got positive append_entries_resp from ~p with match_index ~p",
               [Server, MatchIndex]),

  case maps:get(MatchIndex, ActiveRequests, no_request) of
    no_request ->
      logger:debug("No active request found for index ~p",  [MatchIndex]),
      replicated(Server, MatchIndex, State);
    #active_request{replicated = Replicated} = ActiveRequest ->
      logger:debug("Active request found for index ~p", [MatchIndex]),
      NewReplicated =
        case lists:member(Server, Replicated) of
          true ->
            % repeated msg, no add
            Replicated;
          _ ->
            [Server | Replicated]
        end,
      NewActiveRequest = ActiveRequest#active_request{replicated = NewReplicated},
      case length(NewReplicated) >= raft_cluster:majority_count(Cluster) of
        true ->
          logger:debug("Majority of the logs are successfully replicated for index ~p",
                       [MatchIndex]),
          logger:debug("Replicated server: ~p -> ~p ", [NewReplicated, Cluster]),
          NewState2 = State#state{committed_index = MatchIndex},
          {ok, {_Term, Command}} = raft_log:get(State#state.log, MatchIndex),
          {reply, Reply, NewState3} = execute(Command, NewState2),
          NewActiveRequests = maps:remove(MatchIndex, ActiveRequests),
          NewInnerState = InnerState#leader_state{active_requests = NewActiveRequests},
          NewState4 = NewState3#state{inner_state = NewInnerState, last_applied = MatchIndex},
          From = NewActiveRequest#active_request.from,
          case Command of
            {?JOINT_CLUSTER_CHANGE, {_JointCluster, FinalCluster}} ->
              logger:debug("Joint cluster change has replicated, switching to final config ~p",
                           [FinalCluster]),

              NewState5 = replicated(Server, MatchIndex, NewState4),
              handle_command(From, {?CLUSTER_CHANGE, FinalCluster}, NewState5);
            _ ->
              gen_statem:reply(From, Reply),
              replicated(Server, MatchIndex, NewState4)
          end;
        _ ->
          logger:debug("No majority has reached yet for index ~p", [MatchIndex]),
          NewActiveRequests = ActiveRequests#{MatchIndex => NewActiveRequest},
          replicated(Server, MatchIndex,
                     State#state{inner_state =
                                 InnerState#leader_state{active_requests = NewActiveRequests}})
      end
  end;
handle_append_entries_resp(#append_entries_resp{success = false,
                                                server = Server},
                           #state{inner_state = #leader_state{peers = Peers} = InnerState,
                             log = Log,
                             current_term = Term,
                             committed_index = CommittedIndex
                      } = State) ->
  logger:warning("Got negative appen_entries_resp from ~p", [Server]),
  case maps:get(Server, Peers, not_found) of
    not_found ->
      State;
    Peer ->
      NewPeer = raft_peer:decrease_next_index(Peer),
      send_append_req(NewPeer, Log, Term, CommittedIndex),
      State#state{inner_state = InnerState#leader_state{peers = Peers#{Server => NewPeer}}}
  end.

cancel_all_pending_requests(#leader_state{active_requests = ActiveRequests} = LeaderState) ->
    maps:map(fun(_, #active_request{from = From}) ->
                gen_statem:reply(From, {error, leader_changed})
              end, ActiveRequests),
    LeaderState#leader_state{active_requests = []}.

print_record(#append_entries_req{term = Term,
                                 leader = Leader,
                                 prev_log_index = PLI,
                                 prev_log_term = PLT,
                                 entries = Entries,
                                 leader_commit_index = LCI}) ->
  Msg = "**** Append entries req [~p] ****~n" ++
        "term = ~p~n" ++
        "leader = ~p~n" ++
        "prev_log_index = ~p~n" ++
        "prev_log_term = ~p~n" ++
        "entries = ~p~n" ++
        "leader_commit_index = ~p~n ~n",
  logger:debug(Msg, [self(), Term, Leader, PLI, PLT, Entries, LCI]);
print_record(#append_entries_resp{term = Term,
                                 server = Server,
                                 match_index = MatchIndex,
                                 success = Success}) ->
    Msg = "**** Append entries resp [~p] ****~n" ++
          "term = ~p~n" ++
          "server = ~p~n" ++
          "match_index = ~p~n" ++
          "success = ~p~n ~n",
  logger:debug(Msg, [self(), Term, Server, MatchIndex, Success]);
print_record(#state{current_term = CurrentTerm,
                    cluster = Cluster,
                    voted_for = VotedFor,
                    log = Log,
                    committed_index = CommittedIndex,
                    last_applied = LastApplied,
                    inner_state = InnerState}) ->
  Msg = "**** State [~p] ****~n" ++
        "term = ~p~n" ++
        "leader = ~p~n" ++
        "voted_for = ~p~n" ++
        "cluster members = ~p~n" ++
        "committed_index = ~p~n" ++
        "last_applied = ~p~n" ++
        "log pos = last index: ~p, last_term: ~p~n" ++
        "inner state = ~p~n ~n",
  logger:debug(Msg, [self(), CurrentTerm, raft_cluster:leader(Cluster), VotedFor,
             raft_cluster:members(Cluster), CommittedIndex, LastApplied,
             raft_log:last_index(Log), raft_log:last_term(Log),
             InnerState]).


set_logger_meta(Meta) ->
  NewMeta = case logger:get_process_metadata() of
              undefined ->
                Meta;
              OldMeta ->
                maps:merge(OldMeta, Meta)
            end,
  logger:set_process_metadata(NewMeta).

send_append_reqs(Peers, Log, Term, CommittedIndex) ->
  maps:map(fun(_, Peer) -> send_append_req(Peer, Log, Term, CommittedIndex) end, Peers).

send_append_req(Peer, Log, Term, CommittedIndex) ->
  NextIndex = raft_peer:next_index(Peer),
  Server = raft_peer:server(Peer),
  LastIndex = raft_log:last_index(Log),
  PrevIndex = NextIndex-1,

  PrevLogTerm = case PrevIndex of
                  0 ->
                    Term;
                  LastIndex ->
                    raft_log:last_term(Log);
                  _ ->
                    logger:error("prev index for ~p is ~p", [Server, PrevIndex]),
                    {ok, {LTerm, _Command}} = raft_log:get(Log, PrevIndex),
                    LTerm
                end,

  Commands = get_command(Log, NextIndex),
  logger:debug("Send append log entry to ~p term: ~p prev_log_index: ~p prev_log_term: ~p",
               [Server, Term, PrevIndex, PrevLogTerm]),
  erlang:send_nosuspend(Server, #append_entries_req{term = Term,
                                                    leader = self(),
                                                    prev_log_index = PrevIndex,
                                                    prev_log_term = PrevLogTerm,
                                                    entries = Commands,
                                                    leader_commit_index = CommittedIndex}).

get_command(Log, Index) ->
  case raft_log:get(Log, Index) of
    {ok, {Term, Command}} ->
      [{Term, Command}];
    not_found ->
      []
  end.

maybe_apply_cluster_changes([], State) ->
  State;
maybe_apply_cluster_changes([{_Term, {?CLUSTER_CHANGE, NewCluster}} | Entries], State) ->
  maybe_apply_cluster_changes(Entries, apply_cluster_change(NewCluster, State));
maybe_apply_cluster_changes([{_Term, {?JOINT_CLUSTER_CHANGE, {JointCluster, _FinalCluster}}} | Entries],
                            State) ->
  maybe_apply_cluster_changes(Entries, apply_cluster_change(JointCluster, State));
maybe_apply_cluster_changes([_ | Entries], State) ->
  maybe_apply_cluster_changes(Entries, State).


update_peers(Peers, Cluster, Log) ->
  Members = raft_cluster:members(Cluster) -- [raft_cluster:leader(Cluster)],
  PeerMembers = maps:keys(Peers),

  % remove pees which not included in members
  PeersToRemove = PeerMembers -- Members,
  NewPeers = lists:foldl(fun maps:remove/2, Peers, PeersToRemove),

  % add new peers
  PeersToAdd = Members -- PeerMembers,
  lists:foldl(fun(Server, Acc) ->
                Acc#{Server => raft_peer:new(Server, Log)}
              end, NewPeers, PeersToAdd).


replicated(Server, MatchIndex,
           #state{inner_state = #leader_state{peers = Peers} = LeaderState,
                  committed_index = CommittedIndex,
                  log = Log,
                  current_term = Term} = State) ->
  Self = self(),
  case Server of
    Self ->
      State;
    _ ->
      case maps:get(Server, Peers, not_found) of
        not_found ->
          State;
        Peer ->
          NewPeer = raft_peer:replicated(Peer, MatchIndex),
          case MatchIndex < CommittedIndex of
            true ->
              send_append_req(NewPeer, Log, Term, CommittedIndex);
            _ ->
              ok
          end,
          State#state{inner_state = LeaderState#leader_state{peers = Peers#{Server => NewPeer}}}
      end
  end.