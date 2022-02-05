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
-define(MAX_HEARTBEAT_TIMEOUT, 15000).
-define(MIN_HEARTBEAT_TIMEOUT, 360).
-define(HEARTBEAT_GRACE_TIME, 120).

-define(ELECTION_TIMEOUT(Timeout), {state_timeout, Timeout, election_timeout}).
-define(ELECTION_TIMEOUT, ?ELECTION_TIMEOUT(get_timeout())).

-define(CLUSTER_CHANGE_COMMAND, '$raft_cluster_change_command').
-define(CLUSTER_CHANGE, '$raft_cluster_change').
-define(JOINT_CLUSTER_CHANGE, '$raft_joint_cluster_change').

%% API
-export([start_link/2, stop/1,
         status/1,
         command/2, query/2,
         join/2, leave/2, command/3]).

%% gen_statem callbacks
-export([init/1,
         format_status/2,
         handle_event/4,
         terminate/3,
         code_change/4,
         callback_mode/0]).

-define(SERVER, ?MODULE).

-type(state_name() :: follower | candidate | leader).

-record(follower_state, {}).

-record(candidate_state, {
  granted = 0 :: non_neg_integer()
}).

-record(request, {
  log_index :: log_index(),
  from :: gen_statem:from(),
  majority :: pos_integer(),
  replicated = [] :: [pid()]
}).

-type(active_request() :: #request{}).

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

  committed_index = 0 :: log_index() | 0,
  last_applied = 0 :: log_index() | 0,

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
  prev_log_index :: log_index() | 0,
  prev_log_term :: raft_term(),
  entries :: [command()],
  leader_commit_index :: log_index()
}).

-record(append_entries_resp, {
  term :: raft_term(),
  server :: pid(),
  match_index :: log_index() | 0,
  success :: boolean()
}).

-record(install_snapshot_req, {
  term :: raft_term(),
  leader :: pid(),
  last_included_index :: log_index() | 0,
  last_included_term :: raft_term(),
  cluster :: raft_cluster:cluster(),
  bloom :: binary(),
  user_state :: term()
}).

-callback get_log_module() -> module().
-callback init() -> State when State :: term().
-callback handle_command(Command, State) -> {Reply, State} when
  Command :: term(),
  State :: term(),
  Reply :: term().
-callback handle_query(Command, State) -> Reply when
  Command :: term(),
  State :: term(),
  Reply :: term().

-optional_callbacks([get_log_module/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(binary() | undefined, module()) -> {ok, pid()} | ignore | {error, term()}.
start_link(ServerId, CallbackModule) ->
  gen_statem:start_link(?MODULE, {ServerId, CallbackModule}, []).

-spec stop(pid()) -> ok.
stop(Server) ->
  gen_statem:stop(Server).

-spec status(pid()) -> map().
status(ClusterMember) ->
  {status, _Pid, {module, _Module}, [_, _, _, _, SItems]} = sys:get_status(ClusterMember, 30000),
  proplists:get_value(state, SItems).

-spec join(pid(), pid()) -> ok | {error, no_leader | leader_changed}.
join(ClusterMember, NewClusterMember) ->
  call(ClusterMember, {join, NewClusterMember}, {clean_timeout, 5000}).

-spec leave(pid(), pid()) -> ok | {error, no_leader | leader_changed}.
leave(ClusterMember, MemberToLeave) ->
  call(ClusterMember, {leave, MemberToLeave}, {clean_timeout, 5000}).

-spec command(pid(), term()) ->
  term().
command(ActualClusterMember, Command) ->
  command(ActualClusterMember, generate_req_id(), Command).

-spec command(pid(), req_id(), term()) ->
  term().
command(ActualClusterMember, ReqId, Command) ->
  call(ActualClusterMember, {command, ReqId, Command}, {dirty_timeout, 10000}).

-spec query(pid(), term()) ->
  term().
query(ActualClusterMember, Command) ->
  call(ActualClusterMember, {query, Command}, {dirty_timeout, 10000}).

call(Server, Command, Timeout) ->
  % @TODO store leader in pd to save one call?!
  case catch gen_statem:call(Server, Command, Timeout) of
    {error, {not_leader, undefined}} ->
      %erase({?MODULE, leader, Server}),
      {error, no_leader};
    {error, {not_leader, Pid}} ->
      %put({?MODULE, leader, Server}, Pid),
      case call(Pid, Command, Timeout) of
        {'EXIT', {noproc, _}} ->
          % leader somehow died
          % retry with the original server, it may know who is the new leader
          call(Server, Command, Timeout);
        Else ->
          Else
      end;
    Else ->
      Else
  end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec init(module()) -> {ok, follower, state()}.
init({ServerId, CallbackModule}) ->
  erlang:process_flag(trap_exit, true),
  logger:info("Starting raft server with id: ~p, callback: ~p", [ServerId, CallbackModule]),
  Log = try
          raft_log:new(ServerId, apply(CallbackModule, get_log_module, []))
        catch error:undef:_S ->
          raft_log:new(ServerId, raft_log_ets)
        end,
  {ok, follower, init_user_state(#state{callback = CallbackModule,
                                        log = Log,
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
  LastLogEntries = [begin {ok, {Term, ReqId, Command}} = raft_log:get(Log, Idx),
                          {Idx, Term, ReqId, Command}
                    end || Idx <- lists:seq(erlang:max(1, LastIndex-10), LastIndex)],

  {state, #{role => StateName,
    term => CurrentTerm,
    leader => raft_cluster:leader(Cluster),
    cluster_members => raft_cluster:members(Cluster),
    log_last_index => LastIndex,
    committed_index => Ci,
    last_applied => La,
    log => lists:reverse(LastLogEntries)
   }}.

%% %%%%%%%%%%%%%%%
%% Follower
%% %%%%%%%%%%%%%%%
handle_event(enter, PrevStateName, follower, #state{cluster = Cluster} = State) ->
  set_logger_meta(#{raft_role => follower}),
  logger:info("Became a follower", []),
  case PrevStateName of
    leader -> cancel_all_pending_requests(State#state.inner_state);
    _ -> ok
  end,
  case raft_cluster:majority_count(Cluster) of
    1 ->
      % Alone in cluster, no one will send heartbeat, so step up for candidate (almost) immediately
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT(10)]};
    _ ->
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT]}
  end;
handle_event(state_timeout, election_timeout, follower, #state{cluster = Cluster} = State) ->
  case raft_cluster:majority_count(Cluster) of
    1 -> logger:debug("Heartbeat timeout", []); % alone in the cluster enough to debug log
    _ -> logger:warning("Heartbeat timeout", [])
  end,
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
                                       prev_log_term = PrevLogTerm} = AppendEntriesReq, _StateName,
             #state{current_term = CurrentTerm, log = Log, cluster = Cluster} = State)
  when Term > CurrentTerm ->
  logger:debug("Got append_entries_req with higher Term ~p > ~p", [Term, CurrentTerm]),
  NewState2 =
    case raft_log:get_term(Log, PrevLogIndex) of
      {ok, PrevLogTerm} -> % log exists and the term matched
        NewState = store_entries(AppendEntriesReq, State),
        NewState#state{voted_for = undefined};
      _ when PrevLogIndex =:= 0 -> % nothing in the log yet
        NewState = store_entries(AppendEntriesReq, State),
        NewState#state{voted_for = undefined};
      _ = What->
        logger:notice("Previous log entry ~p not matched reply false -> ~p", [PrevLogIndex, What]),
        % reply false, and step down
        send_msg(Leader, #append_entries_resp{term = Term,
                                              server = self(),
                                              success = false,
                                              match_index = raft_log:last_index(Log)}),
        maybe_apply_cluster_changes(Entries,
                                    State#state{current_term = Term,
                                                cluster = raft_cluster:leader(Cluster, Leader)})
    end,
  {next_state, follower, NewState2#state{voted_for = undefined}, [?ELECTION_TIMEOUT]};

% append entries with same term
handle_event(info, #append_entries_req{term = Term,
                                       leader = Leader,
                                       entries = Entries,
                                       prev_log_index = PrevLogIndex,
                                       prev_log_term = PrevLogTerm} = AppendEntriesReq, StateName,
                    #state{current_term = Term, cluster = Cluster, log = Log} = State) ->
  logger:debug("Got append_entries_req with ~p term, prev_log_index ~p", [Term, PrevLogIndex]),
  NewState =
    case raft_log:get_term(Log, PrevLogIndex) of
      {ok, PrevLogTerm} -> % log exists and the term matched
        store_entries(AppendEntriesReq, State);
      _ when PrevLogIndex =:= 0 -> % nothing in the log yet
        store_entries(AppendEntriesReq, State);
      _ = What ->
        logger:notice("Previous log entry ~p not matched reply false -> ~p", [PrevLogIndex, What]),
        % reply false, and step down
        send_msg(Leader, #append_entries_resp{term = Term,
                                              server = self(),
                                              success = false,
                                              match_index = raft_log:last_index(Log)}),
        maybe_apply_cluster_changes(Entries,
                                    State#state{current_term = Term,
                                                cluster = raft_cluster:leader(Cluster, Leader)})
    end,
  case StateName of
    leader -> {next_state, follower, NewState, [?ELECTION_TIMEOUT]};
    _ -> {keep_state, NewState, [?ELECTION_TIMEOUT]}
  end;

handle_event(info, #install_snapshot_req{term = Term, leader = Leader}, _StateName,
             #state{current_term = CurrentTerm}) when Term < CurrentTerm ->
  logger:notice("Got install_snapshot_req with lower Term ~p < ~p, rejecting", [Term, CurrentTerm]),
  send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                        server = self(),
                                        success = false,
                                        match_index = 0}),
  keep_state_and_data;

handle_event(info, #install_snapshot_req{term = Term,
                                         leader = Leader,
                                         last_included_index = LastIndex,
                                         last_included_term = LastTerm,
                                         cluster = Cluster,
                                         bloom = Bloom,
                                         user_state = UserState},
             _StateName,
  #state{current_term = CurrentTerm, log = Log} = State)
  when Term > CurrentTerm ->
  logger:debug("Got install_snapshot_req with higher Term ~p > ~p", [Term, CurrentTerm]),
  NewLog1 = raft_log:delete(Log, LastIndex),
  NewLog2 = raft_log:store_snapshot(NewLog1, LastIndex, LastTerm, Bloom, UserState),

  NewState = State#state{user_state = UserState, log = NewLog2,
                         last_applied = LastIndex, cluster = Cluster
  },
  send_msg(Leader, #append_entries_resp{term = Term,
                                        server = self(),
                                        success = true,
                                        match_index = raft_log:last_index(NewLog2)}),
  {next_state, follower, NewState#state{voted_for = undefined}, [?ELECTION_TIMEOUT]};

handle_event(info, #install_snapshot_req{term = Term,
                                         leader = Leader,
                                         last_included_index = LastIndex,
                                         last_included_term = LastTerm,
                                         cluster = Cluster,
                                         bloom = Bloom,
                                         user_state = UserState}, StateName,
  #state{current_term = Term, log = Log} = State) ->
  logger:debug("Got install_snapshot_req", []),
  NewLog1 = raft_log:delete(Log, LastIndex),
  NewLog2 = raft_log:store_snapshot(NewLog1, LastIndex, LastTerm, Bloom, UserState),
  NewState = State#state{user_state = UserState, log = NewLog2,
                         last_applied = LastIndex, cluster = Cluster
  },
  send_msg(Leader, #append_entries_resp{term = Term,
                                        server = self(),
                                        success = true,
                                        match_index = raft_log:last_index(NewLog2)}),
  case StateName of
    leader -> {next_state, follower, NewState, [?ELECTION_TIMEOUT]};
    _ -> {keep_state, NewState, [?ELECTION_TIMEOUT]}
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
      logger:notice("Got vote_req for ~p, accepting",
                    [Candidate]),
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
  set_logger_meta(#{raft_role => leader}),
  logger:info("Become leader", []),
  NewCluster = raft_cluster:leader(Cluster, self()),
  NewState = State#state{cluster = NewCluster,
                         inner_state = #leader_state{peers = update_peers(#{}, NewCluster, Log)}},

  % Upon election: send initial empty AppendEntries RPCs
  % (heartbeat) to each server; repeat during idle periods to
  % prevent election timeouts (§5.2)
  NewState2 = send_heartbeat(NewState),
  {keep_state, NewState2, [?ELECTION_TIMEOUT(get_min_timeout())]};
handle_event(state_timeout, election_timeout, leader, State) ->
  NewState = send_heartbeat(State),
  {keep_state, NewState, [?ELECTION_TIMEOUT(get_idle_timeout())]};

handle_event({call, From}, {command, ReqId, Command}, leader, #state{log = Log} = State) ->
  case raft_log:is_logged(Log, ReqId) of
    true ->
      gen_statem:reply(From, {error, {request_already_append_to_log, ReqId}}),
      {keep_state, State, [?ELECTION_TIMEOUT(get_min_timeout())]};
    false ->
      {keep_state,
       handle_command(From, ReqId, Command, State),
       [?ELECTION_TIMEOUT(get_min_timeout())]}
  end;
handle_event({call, From}, {query, Command}, leader, #state{callback = CB, user_state = Us}) ->
  gen_statem:reply(From, apply(CB, handle_query, [Command, Us])),
  keep_state_and_data;
handle_event({call, From}, {join, Server}, leader, #state{cluster = Cluster} = State) ->
  case raft_cluster:join(Server, Cluster) of
    {ok, NewCluster} ->
      logger:info("Join request for ~p server", [Server]),
      {keep_state,
       handle_command(From, generate_req_id(), {?CLUSTER_CHANGE_COMMAND, NewCluster}, State),
       [?ELECTION_TIMEOUT(get_min_timeout())]};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;
handle_event({call, From}, {leave, Server}, leader,
             #state{cluster = Cluster} = State) ->
  case raft_cluster:leave(Server, Cluster) of
    {ok, NewCluster} ->
      logger:info("Leave request for ~p server", [Server]),
      {keep_state,
       handle_command(From, generate_req_id(), {?CLUSTER_CHANGE_COMMAND, NewCluster}, State),
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
handle_event({call, From}, {command, _ReqId, _Command}, _StateName,
             #state{cluster = Cluster}) ->
    gen_statem:reply(From, {error, {not_leader, raft_cluster:leader(Cluster)}}),
    keep_state_and_data;
handle_event({call, From}, {query, _Command}, _StateName,
             #state{cluster = Cluster}) ->
  gen_statem:reply(From, {error, {not_leader, raft_cluster:leader(Cluster)}}),
  keep_state_and_data;

handle_event({call, From}, {Type, _Node}, _StateName,
             #state{cluster = Cluster}) when Type =:= join orelse Type =:= leave ->
  gen_statem:reply(From, {error, {not_leader, raft_cluster:leader(Cluster)}}),
  keep_state_and_data;

% not leader, can be dropped?
handle_event(info, #append_entries_resp{server = Server}, _StateName, _State) ->
  logger:notice("Got unhandled append entries response from ~p, dropped", [Server]),
  keep_state_and_data;
handle_event(info, #vote_req_reply{} = Vrp, _StateName, _State) ->
  logger:notice("Got unhandled vote_req_reply, dropped ~p ", [Vrp]),
  keep_state_and_data;

handle_event(info, {'EXIT', Server, _Reason}, _StateName, #state{cluster = Cluster} = State) ->
  case raft_cluster:is_member(Server, Cluster) of
    true ->
      % if the leader died, start new consensus
      case Server =:= raft_cluster:leader(Cluster) of
        true ->
          logger:error("Cluster leader ~p died", [Server]),
          % all servers might got this message at almost same time,
          % to give more chance for a success leader election wait random
          timer:sleep(rand:uniform(50)),
          {next_state, candidate, State#state{cluster = raft_cluster:leader(Cluster, undefined)}};
        _ ->
          logger:error("Cluster member ~p died", [Server]),
          keep_state_and_data
      end;
    _ ->
      keep_state_and_data
  end.
%;
%handle_event(EventType, EventContent, StateName, #state{} = State) ->
%  logger:warning("Unhandled event: Et: ~p Ec: ~p~nSt: ~p Sn: ~p~n",
%                 [StateName, EventType, EventContent, State, StateName]),
%  keep_state_and_data.

%% @private
%% @doc This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
terminate(normal, _StateName, #state{log = Log} = _State) ->
  % @TODO in case of normal stop say goodbye to other cluster members!?
  raft_log:destroy(Log),
  ok;
terminate(_Reason, _StateName, #state{} = _State) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State = #state{}, _Extra) ->
  {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
send_heartbeat(State) ->
  send_append_reqs(State).

send_append_reqs(#state{log = Log, current_term = Term, committed_index = CommittedIndex,
                        cluster = Cluster,
                        inner_state = #leader_state{peers = Peers} = InnerState} = State) ->
  NewPeers = send_append_reqs(Peers, Log, Term, CommittedIndex, Cluster),
  State#state{inner_state = InnerState#leader_state{peers = NewPeers}}.

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

get_idle_timeout() ->
  Max = application:get_env(raft, max_heartbeat_timeout, ?MAX_HEARTBEAT_TIMEOUT),
  GraceTime = application:get_env(raft, heartbeat_grace_time, ?HEARTBEAT_GRACE_TIME),
  % now we hase 2x Grace to to send hb
  (Max-GraceTime) + rand:uniform(5).

init_user_state(#state{callback = CallbackModule} = State) ->
  State#state{user_state = apply(CallbackModule, init, [])}.

execute({?CLUSTER_CHANGE, _NewCluster}, State) ->
  {ok, State};
execute({?JOINT_CLUSTER_CHANGE, {_JointCluster, _FinalCluster}}, State) ->
  {ok, State};
execute(Command, #state{callback = CallbackModule, user_state = UserState} = State) ->
  logger:debug("Execute log command ~p", [Command]),
  % @TODO: while apply command gen_server should be able to respond
  % (execute command might take awhile, in worst case more than heartbeat timeout)
  case apply(CallbackModule, handle_command, [Command, UserState]) of
    {Reply, NewUserState} ->
      {Reply, State#state{user_state = NewUserState}}
  end.

apply_cluster_change(NewCluster, #state{cluster = Cluster} = State) ->
  case raft_cluster:term(NewCluster) > raft_cluster:term(Cluster) of
    true ->
      logger:info("Appling new cluster config ~p", [NewCluster]),
      % Unlink removed node?! or just ignore them when they die?
      case raft_cluster:is_member(self(), NewCluster) of
        true ->
          [link(C) || C <- raft_cluster:members(NewCluster), C =/= self()],
          %remove_peers_not_in_cluster(State#state{cluster = NewCluster});
          State#state{cluster = NewCluster};
        false ->
          % leaving the cluster, alone in new cluster :(
          %@TODO should stop or not?
          State#state{cluster = raft_cluster:new()}
      end;
    _ ->
      logger:debug("Igonring cluster config change, it is with lower term ~p ~p",
                   [NewCluster, Cluster]),
      State
  end.

send_msg(Target, Msg) ->
  erlang:send_nosuspend(Target, Msg).

maybe_delete_log(Log, Index, Term) ->
  case raft_log:get_term(Log, Index) of
    {ok, LogTerm} when Term =/= LogTerm ->
      % 3. If an existing entry conflicts with a new one (same index
      % but different terms), delete the existing entry and all that follow it (§5.3)
      raft_log:delete(Log, Index);
    _ ->
      Log
  end.

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
    {ok, {_Term, _ReqId, {?CLUSTER_CHANGE, NewCluster}}} ->
      maybe_apply(State#state{last_applied = CurrentIndex, cluster = NewCluster});
    {ok, {_Term, _ReqId, {?JOINT_CLUSTER_CHANGE, {JointCluster, _FinalCluster}}}} ->
      maybe_apply(State#state{last_applied = CurrentIndex, cluster = JointCluster});
    {ok, {_Term, _ReqId, Command}} ->
      {_, NewState} = execute(Command, State),
      maybe_apply(maybe_store_snapshot(NewState#state{last_applied = CurrentIndex}))
  end;
maybe_apply(State) ->
  State.

handle_command(From, ReqId, {?CLUSTER_CHANGE_COMMAND, NewCluster} = Command,
               #state{log = Log,
                      cluster = CurrentCluster,
                      current_term = Term,
                      inner_state = #leader_state{active_requests = CurrentRequests,
                                                  peers = Peers} = LeaderState
               } = State) ->
  logger:debug("Handling cluster change (joint step): ~p", [Command]),
  JointCluster = raft_cluster:joint_cluster(CurrentCluster, NewCluster),
  FinalCluster = raft_cluster:increase_term(NewCluster),
  JointConsensusClusterChangeCommand = {?JOINT_CLUSTER_CHANGE, {JointCluster, FinalCluster}},
  NewLog = raft_log:append(Log, ReqId, JointConsensusClusterChangeCommand, Term),
  NewLeaderState = LeaderState#leader_state{peers = update_peers(Peers, JointCluster, NewLog)},
  NewState = State#state{log = NewLog, inner_state = NewLeaderState, cluster = JointCluster},
  NewState2 = send_append_reqs(NewState),

  LogIndex = raft_log:last_index(NewLog),
  ActiveRequest = #request{log_index = LogIndex,
                           from = From,
                           majority = raft_cluster:majority_count(JointCluster)},
  NewCurrentActiveRequests = CurrentRequests#{LogIndex => ActiveRequest},
  NewLeaderState2 = NewLeaderState#leader_state{active_requests = NewCurrentActiveRequests},

  SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                               server = self(),
                                               match_index = LogIndex,
                                               success = true},
  handle_append_entries_resp(SelfAppendEntriesResp, NewState2#state{inner_state = NewLeaderState2});
handle_command(From, ReqId, {?CLUSTER_CHANGE, NewCluster} = Command,
               #state{log = Log,
                      current_term = Term,
                      inner_state = #leader_state{active_requests = CurrentRequests,
                                                  peers = Peers} = LeaderState
               } = State) ->
  logger:debug("Handling final cluster change: ~p", [Command]),
  NewLog = raft_log:append(Log, ReqId, {?CLUSTER_CHANGE, NewCluster}, Term),
  NewLeaderState = LeaderState#leader_state{peers = update_peers(Peers, NewCluster, NewLog)},
  NewState = State#state{log = NewLog, inner_state = NewLeaderState, cluster = NewCluster},
  NewState2 = send_append_reqs(NewState),

  % If leader is not in the new cluster after updating the peers should stop sending heartbeats
  case raft_cluster:is_member(self(), NewCluster) of
    true ->
      LogIndex = raft_log:last_index(NewLog),
      ActiveRequest = #request{log_index = LogIndex,
                               from = From,
                               majority = raft_cluster:majority_count(NewCluster)},
      NewCurrentActiveRequests = CurrentRequests#{LogIndex => ActiveRequest},
      NewLeaderState2 = NewLeaderState#leader_state{active_requests = NewCurrentActiveRequests},
      SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                                   server = self(),
                                                   match_index = LogIndex,
                                                   success = true},
      handle_append_entries_resp(SelfAppendEntriesResp, NewState2#state{inner_state = NewLeaderState2});
    false ->
      NewClusterForLeader = raft_cluster:new(),
      NewLeaderStateForLeader =
        LeaderState#leader_state{peers = update_peers(Peers, NewClusterForLeader, NewLog)},
      gen_statem:reply(From, ok),
      State#state{log = NewLog, inner_state = NewLeaderStateForLeader, cluster = NewClusterForLeader}
  end;
handle_command(From, ReqId, Command,
               #state{log = Log,
                      current_term = Term,
                      cluster = Cluster,
                      inner_state = #leader_state{active_requests = CurrentRequests} = LeaderState
               } = State) ->
  logger:debug("Handling command: ~p", [Command]),

  NewLog = raft_log:append(Log, ReqId, Command, Term),
  NewState = State#state{log = NewLog},
  NewState2 = send_append_reqs(NewState),

  LogIndex = raft_log:last_index(NewLog),
  ActiveRequest = #request{log_index = LogIndex,
                           from = From,
                           majority = raft_cluster:majority_count(Cluster)},
  NewCurrentActiveRequests = CurrentRequests#{LogIndex => ActiveRequest},
  NewLeaderState = LeaderState#leader_state{active_requests = NewCurrentActiveRequests},

  SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                               server = self(),
                                               match_index = LogIndex,
                                               success = true},
  handle_append_entries_resp(SelfAppendEntriesResp, NewState2#state{inner_state = NewLeaderState}).


handle_append_entries_resp(#append_entries_resp{success = true,
                                                server = Server,
                                                match_index = MatchIndex},
                           #state{inner_state = #leader_state{active_requests = ActiveRequests} = InnerState
                          } = State) ->
  logger:debug("Got positive append_entries_resp from ~p with match_index ~p",
               [Server, MatchIndex]),

  case handle_request(Server, ActiveRequests, MatchIndex) of
    {replicated, NewActiveRequests, IndexesToCommit} ->

      NewInnerState = InnerState#leader_state{active_requests = NewActiveRequests},
      NewState = State#state{inner_state = NewInnerState},

      lists:foldr(fun({Index, From}, CState) ->
                    commit_index(Server, CState, Index, From)
                  end,
                  NewState, IndexesToCommit);
    NewActiveRequests ->
      replicated(Server, MatchIndex,
                 State#state{inner_state =
                             InnerState#leader_state{active_requests = NewActiveRequests}})
  end;
handle_append_entries_resp(#append_entries_resp{success = false,
                                                server = Server,
                                                match_index = MatchIndex},
                           #state{inner_state = #leader_state{peers = Peers} = InnerState,
                                  log = Log,
                                  current_term = Term,
                                  cluster = Cluster,
                                  committed_index = CommittedIndex
                          } = State) ->
  logger:notice("Got negative appen_entries_resp from ~p with ~p match_index",
                [Server, MatchIndex]),
  case maps:get(Server, Peers, not_found) of
    not_found ->
      logger:notice("Got negative appen_entries_resp from unknown server ~p -> ~p",
                    [Server, Peers]),
      State;
    Peer ->
      NewPeer = raft_peer:next_index(Peer, MatchIndex+1),
      NewPeer2 = send_append_req(NewPeer, Log, Term, CommittedIndex, Cluster),
      State#state{inner_state = InnerState#leader_state{peers = Peers#{Server => NewPeer2}}}
  end.

cancel_all_pending_requests(#leader_state{active_requests = ActiveRequests} = LeaderState) ->
    maps:map(fun(_, #request{from = From}) ->
                gen_statem:reply(From, {error, leader_changed})
              end, ActiveRequests),
    LeaderState#leader_state{active_requests = #{}}.

set_logger_meta(Meta) ->
  NewMeta = case logger:get_process_metadata() of
              undefined ->
                Meta;
              OldMeta ->
                maps:merge(OldMeta, Meta)
            end,
  logger:set_process_metadata(NewMeta).

send_append_reqs(Peers, Log, Term, CommittedIndex, Cluster) ->
  maps:map(fun(_, Peer) -> send_append_req(Peer, Log, Term, CommittedIndex, Cluster) end, Peers).

send_append_req(Peer, Log, Term, CommittedIndex, Cluster) ->
  NextIndex = raft_peer:next_index(Peer),
  Server = raft_peer:server(Peer),
  PrevIndex = NextIndex-1,

  MaxChunk = application:get_env(raft, max_append_entries_chunk_size, 100),
  case raft_log:list(Log, NextIndex, MaxChunk) of
    {ok, EndIndex, Entries} ->
      PrevLogTerm = raft_log:get_term(Log, PrevIndex, Term),
      logger:debug("Send append log entry to ~p term: ~p prev_log_index: ~p prev_log_term: ~p end_index: ~p",
                   [Server, Term, PrevIndex, PrevLogTerm, EndIndex]),
      AppendEntriesReq = #append_entries_req{term = Term,
                                             leader = self(),
                                             prev_log_index = PrevIndex,
                                             prev_log_term = PrevLogTerm,
                                             entries = Entries,
                                             leader_commit_index = CommittedIndex},
      send_msg(Server, AppendEntriesReq),
      raft_peer:next_index(Peer, EndIndex+1);
    {snapshot, {SnapshotLastTerm, LastLogIndex, Bloom, UserState}} ->
      logger:debug("Send install snapshot req to ~p last_index: ~p last_term: ~p",
                   [Server, LastLogIndex, SnapshotLastTerm]),
      SnapshotReq = #install_snapshot_req{term = Term,
                                          leader = self(),
                                          last_included_index = LastLogIndex,
                                          last_included_term = SnapshotLastTerm,
                                          cluster = Cluster,
                                          bloom = Bloom,
                                          user_state = UserState},
      send_msg(Server, SnapshotReq),
      raft_peer:next_index(Peer, LastLogIndex+1)
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

replicated(Server, _MatchIndex, State) when Server =:= self() ->
  State;
replicated(Server, MatchIndex,
           #state{inner_state = #leader_state{peers = Peers} = LeaderState,
                  committed_index = CommittedIndex,
                  log = Log,
                  cluster = Cluster,
                  current_term = Term} = State) ->
  case maps:get(Server, Peers, not_found) of
    not_found ->
      State;
    Peer ->
      NewPeer = raft_peer:replicated(Peer, MatchIndex),
      NewPeer2 = case raft_peer:next_index(NewPeer) =< CommittedIndex of
                    true ->
                      send_append_req(NewPeer, Log, Term, CommittedIndex, Cluster);
                    _ ->
                      NewPeer
                  end,
      State#state{inner_state = LeaderState#leader_state{peers = Peers#{Server => NewPeer2}}}
  end.

handle_request(Server, ActiveRequests, MatchIndex) ->
  case maps:get(MatchIndex, ActiveRequests, no_request) of
    no_request ->
      logger:debug("No active request found for index ~p",  [MatchIndex]),
      ActiveRequests;
    #request{replicated = Replicated} = ActiveRequest ->
      logger:debug("Active request found for index ~p -> ~p", [MatchIndex, ActiveRequest]),
      NewReplicated = case lists:member(Server, Replicated) of
                        true -> Replicated;
                        _ -> [Server | Replicated]
                      end,
      NewActiveRequest = ActiveRequest#request{replicated = NewReplicated},
      NewActiveRequests = ActiveRequests#{MatchIndex => NewActiveRequest},
      case replicated_indexes(NewActiveRequests, MatchIndex) of
        {UpdatedNewActiveRequest, []} ->
          UpdatedNewActiveRequest;
        {UpdatedNewActiveRequest, IndexesToCommit} ->
          {replicated, UpdatedNewActiveRequest, IndexesToCommit}
      end
  end.

replicated_indexes(Requests, Index) ->
  replicated_indexes(Requests, Index, []).

replicated_indexes(Requests, Index, Acc) ->
  case maps:get(Index, Requests, not_found) of
    #request{replicated = Replicated, majority = Majority}
      when length(Replicated) >= Majority ->
      logger:debug("Majority of the logs are successfully replicated for index ~p", [Index]),
      % Clean all the lower indexes because if have majority replicated all the lower
      % requests surely replicated
      {NewRequests, NewAcc} = clean_requests(Index, Requests, Acc),
      replicated_indexes(NewRequests, Index+1, NewAcc);
    not_found when map_size(Requests) > 0 ->
      NextActiveRequestIndex = lists:min(maps:keys(Requests)),
      replicated_indexes(Requests, NextActiveRequestIndex, Acc);
    _  ->
      {Requests, lists:reverse(Acc)}
  end.

clean_requests(Index, Request, Acc) ->
  case maps:get(Index, Request, not_found) of
    not_found ->
      {Request, Acc};
    #request{from = From} ->
      clean_requests(Index-1, maps:remove(Index, Request), [{Index, From} | Acc])
  end.

commit_index(Server, State, Index, From) ->
  NewState2 = State#state{committed_index = Index},
  {ok, {_Term, _ReqId, Command}} = raft_log:get(State#state.log, Index),
  {Reply, NewState3} = execute(Command, NewState2),
  NewState4 = maybe_store_snapshot(NewState3#state{last_applied = Index}),
  case Command of
    {?JOINT_CLUSTER_CHANGE, {_JointCluster, FinalCluster}} ->
      logger:info("Joint cluster change has replicated, switching to final config ~p",
                  [FinalCluster]),

      NewState5 = replicated(Server, Index, NewState4),
      handle_command(From, generate_req_id(), {?CLUSTER_CHANGE, FinalCluster}, NewState5);
    _ ->
      gen_statem:reply(From, Reply),
      replicated(Server, Index, NewState4)
  end.

maybe_store_snapshot(#state{log = Log, last_applied = LastApplied} = State) ->
  %@TODO better log compaction trigger logic
  SnapshotChunkSize = application:get_env(raft, snapshot_chunk_size, 100000),
  NewLog = case LastApplied rem SnapshotChunkSize of
              0 when LastApplied > 1 ->
                raft_log:store_snapshot(Log, LastApplied, State#state.user_state);
                %Log;
              _ ->
                Log
            end,
  State#state{log = NewLog}.

store_entries(#append_entries_req{term = Term,
                                  leader = Leader,
                                  entries = Entries,
                                  prev_log_index = PrevLogIndex,
                                  leader_commit_index = LeaderCommitIndex},
              #state{committed_index = CommittedIndex, log = Log, cluster = Cluster} = State) ->
  NextLogIndex = PrevLogIndex+1,
  NewLog = maybe_delete_log(Log, NextLogIndex, Term),
  NewLog2 = raft_log:append_commands(NewLog, Entries, NextLogIndex),
  logger:debug("AppendEntries successfully commited on index ~p", [raft_log:last_index(NewLog2)]),
  send_msg(Leader, #append_entries_resp{term = Term,
                                        server = self(),
                                        success = true,
                                        match_index = raft_log:last_index(NewLog2)}),
  NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CommittedIndex, NewLog2),
  NewState = State#state{current_term = Term,
                         log = NewLog2,
                         cluster = raft_cluster:leader(Cluster, Leader),
                         committed_index = NewCommitIndex},
  maybe_apply_cluster_changes(Entries, maybe_apply(NewState)).

generate_req_id() ->
  T = erlang:integer_to_binary(os:system_time()),
  N = atom_to_binary(node()),
  U = erlang:integer_to_binary(erlang:unique_integer([positive])),
  <<"generated-", N/binary, "-", T/binary, "-", U/binary>>.
