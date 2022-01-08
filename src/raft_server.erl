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
-define(MIN_HEARTBEAT_TIMEOUT, 1500).
-define(HEARTBEAT_GRACE_TIME, 1500).

-define(ELECTION_TIMEOUT(Timeout), {state_timeout, Timeout, election_timeout}).
-define(ELECTION_TIMEOUT, ?ELECTION_TIMEOUT(get_timeout())).

-define(CLUSTER_CHANGE, '$raft_cluster_change').

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

-type(state_name() :: follower | candidate | leader).

-record(follower_state, {
  %committed_index :: log_index()
}).

-record(candidate_state, {
  granted = 0 :: non_neg_integer()
}).

-record(active_request, {
  log_index :: log_index(),
  from :: gen_statem:from(),
  majority :: pos_integer(),
  replicated = [] :: [pid()]
}).

-type(active_request() :: #active_request{}).

-record(raft_peer, {
    server :: pid(),
    next_index :: log_index(),
    match_index :: log_index() | 0
}).

-type(raft_peer() :: #raft_peer{}).

-record(leader_state, {
    peers = #{} :: #{pid() => raft_peer()},
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
  gen_statem:call(ClusterMember, status, 30000).

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
  logger:info("Starting raft server: ~p", [CallbackModule]),
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
format_status(_Opt, [_PDict, StateName, _State]) ->
  {_Opt, [_PDict, StateName, _State]}.

%% %%%%%%%%%%%%%%%
%% Follower
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, follower, #state{cluster = Cluster} = State) ->
  logger:info("Became a follower", []),
  set_logger_meta(#{raft_role => follower}),
  case _PrevStateName of
    leader ->
      cancel_all_pending_requests(State#state.inner_state);
    _ ->
      ok
  end,
  case raft_cluster:majority_count(Cluster) of
    1 ->
      % Alone in cluster, no one will send heartbeat, so step up for candidate immediately
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT(0)]};
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
  logger:info("Became a candidate", []),
  set_logger_meta(#{raft_role => candidate}),

  % To begin an election, a follower increments its current term and transitions to candidate state
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
  send_msg(Me, #vote_req_reply{term = NewTerm, granted = true}),
  logger:debug("Send vote request for term ~p", [Term]),

  [send_msg(Server, VoteReq) || Server <- raft_cluster:members(Cluster), Server =/= Me],

  NewState = State#state{current_term = NewTerm, inner_state = #candidate_state{},
                         cluster = raft_cluster:leader(Cluster, undefined),
                         voted_for = undefined},
  {keep_state, NewState, [?ELECTION_TIMEOUT]};

% process vote reply
handle_event(info, #vote_req_reply{term = Term, granted = true}, candidate,
             #state{current_term = Term,
                    cluster = Cluster,
                    inner_state = InnerState = #candidate_state{granted = Granted}} = State) ->
  NewInnerState = InnerState#candidate_state{granted = Granted+1},
  logger:debug("Got positive vote reply ~p", []),
  Majority = raft_cluster:majority_count(Cluster),
  case NewInnerState of
    #candidate_state{granted = Granted} when Granted < Majority ->
      {keep_state, State#state{inner_state = NewInnerState}};
    _ ->
      logger:info("Have the majority of the votes", []),
      {next_state, leader, State#state{inner_state = NewInnerState}}
  end;
handle_event(info, #vote_req_reply{term = Term, granted = false}, candidate,
             State = #state{current_term = CurrentTerm}) when Term > CurrentTerm ->
  logger:notice("Vote request reply contains higher term", []),
  {next_state, follower, State#state{current_term = Term, voted_for = undefined}};
handle_event(info, #vote_req_reply{}, candidate, State) ->
  logger:notice("Vote request ignored", []),
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
handle_event(info, #append_entries_req{term = Term, leader = Leader}, _StateName,
             #state{current_term = CurrentTerm}) when Term < CurrentTerm ->
  logger:notice("Got append_entries_req with lower Term ~p < ~p, rejecting", [Term, CurrentTerm]),
  send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                        server = self(),
                                        success = false,
                                        match_index = 0}),
  keep_state_and_data;
handle_event(info, #append_entries_req{term = Term, leader = Leader}, _StateName,
             #state{current_term = CurrentTerm, log = Log, cluster = Cluster} = State)
  when Term > CurrentTerm ->
  logger:warning("Got append_entries_req with higher Term ~p > ~p, rejecting and step down",
                 [Term, CurrentTerm]),
  send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                        server = self(),
                                        success = false,
                                        match_index = raft_log:last_index(Log)}),
  {next_state, follower, State#state{current_term = Term,
                                     voted_for = undefined,
                                     cluster = raft_cluster:leader(Cluster, Leader)}};
handle_event(info,
             #append_entries_req{prev_log_index = PrevLogIndex, leader = Leader, term = Term,
                                 leader_commit_index = LeaderCommitIndex} = AppendReq,
             _StateName,
             #state{log = Log, current_term = CurrentTerm, committed_index = CI} = State) ->
  print_record(State),
  print_record(AppendReq),
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {LogTerm, _Command}} when Term =/= LogTerm ->
      logger:warning("Got append_entries_req with different prev_log_term ~p =/= ~p, rejecting",
                     [Term, LogTerm]),
      send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                            server = self(),
                                            success = false,
                                            match_index = 0}),
      {keep_state, State};
    not_found when PrevLogIndex =/= 0->
      logger:warning("Got append_entries_req prev_log_index ~p entry not found, rejecting",
                     [PrevLogIndex]),
      send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                            server = self(),
                                            success = false,
                                            match_index = PrevLogIndex-1}),
      {keep_state, State};
    _ ->
      NewLog = append_commands(AppendReq, Log),

      NewState1 = State,
      logger:debug("Got append_entries_req index, accepted", []),
      send_msg(Leader, #append_entries_resp{term = CurrentTerm,
                                            match_index = raft_log:last_index(NewLog),
                                            server = self(),
                                            success = true}),

      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CI, NewLog),
      NewState2 = NewState1#state{log = NewLog, committed_index = NewCommitIndex},
      print_record(NewState2),
      case AppendReq#append_entries_req.entries of
        [{?CLUSTER_CHANGE, NewCluster}] ->
          apply_cluster_change(NewCluster, NewState2);
        _ ->
          case maybe_apply(NewState2) of
            {ok, NewState3} ->
              {keep_state, NewState3, [?ELECTION_TIMEOUT]};
            {next_state, _NextStateName, _NewState4} = Result ->
              Result
          end
      end
  end;
% handle vote requests
% If votedFor is null or candidateId, and candidate’s log is at
% least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm} = State) when Term < CurrentTerm ->
  logger:notice("Got vote_req for ~p with lower term ~p < ~p, rejecting",
                 [Candidate, Term, CurrentTerm]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false}),
  {keep_state, State};
handle_event(info, #vote_req{term = Term, candidate = Candidate,
                             last_log_index = LastIndex, last_log_term = LastTerm}, _StateName,
             #state{current_term = CurrentTerm, log = Log} = State) when Term > CurrentTerm ->
  case {raft_log:last_index(Log), raft_log:last_term(Log)} of
      {LastIndex, LastTerm} ->
          % up-to-date log
          logger:info("Got vote_req for ~p with higher term ~p > ~p, accepted",
                      [Candidate, Term, CurrentTerm]),
          send_msg(Candidate, #vote_req_reply{term = Term, granted = true}),
          {next_state, follower, State#state{current_term = Term, voted_for = Candidate}};
      _ ->
        logger:warning("Got vote_req for ~p with higher term ~p > ~p, log not up-to-date, rejected",
                       [Candidate, Term, CurrentTerm]),
        send_msg(Candidate, #vote_req_reply{term = Term, granted = false}),
        {next_state, follower, State#state{current_term = Term, voted_for = Candidate}}
  end;
handle_event(info, #vote_req{candidate = Candidate,
                             last_log_term = LastTerm, last_log_index = LastIndex}, _StateName,
             #state{current_term = CurrentTerm, voted_for = undefined, log = Log} = State) ->

    case {raft_log:last_index(Log), raft_log:last_term(Log)} of
        {LastIndex, LastTerm} ->
          logger:info("Got vote_req for ~p, accepted", [Candidate]),
          send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = true});
        _ ->
          logger:warning("Got vote_req for ~p, log not up-to-date, rejected",
                           [Candidate]),
          send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false})
    end,
  {keep_state, State#state{voted_for = Candidate}};
handle_event(info, #vote_req{candidate = Candidate,
                             last_log_index = LastIndex, last_log_term = LastTerm}, _StateName,
             #state{current_term = CurrentTerm, voted_for = Candidate, log = Log} = State) ->
    case {raft_log:last_index(Log), raft_log:last_term(Log)} of
        {LastIndex, LastTerm} ->
          logger:info("Got vote req for ~p, already voted for the same candidate, accepted",
                      [Candidate]),
          send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = true});
        _ ->
          logger:warning("Got vote_req for ~p, log not up-to-date, rejected",
                         [Candidate]),
            send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false})
    end,
   {keep_state, State};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = _OtherCandidate} = State) ->
  logger:debug("Got vote_req for ~p, already voted in term ~p, rejected",
               [Candidate, CurrentTerm]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false}),
  {keep_state, State};

handle_event({call, From}, status, StateName,
             #state{current_term = CurrentTerm, cluster = Cluster,
                    committed_index = Ci,
                    last_applied = La}) ->
  Status = #{role => StateName,
             term => CurrentTerm,
             leader => raft_cluster:leader(Cluster),
             cluster_members => raft_cluster:members(Cluster),
             committed_index => Ci,
             last_applied => La
            },
  gen_statem:reply(From, Status),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%
%% Leader
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, leader, #state{cluster = Cluster, log = Log} = State) ->
  logger:info("Become leader", []),
  set_logger_meta(#{raft_role => leader}),
  NewCluster = raft_cluster:leader(Cluster, self()),
  NewState = State#state{cluster = NewCluster,
                         inner_state = #leader_state{peers = raft_peer:new(NewCluster, Log)}},

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
             #state{cluster = Cluster, log = Log,
                    inner_state = #leader_state{peers = Peers} = LeaderState} = State) ->
  case raft_cluster:join(Server, Cluster) of
    {ok, NewCluster} ->
      NewLeaderState = LeaderState#leader_state{
          peers = Peers#{Server => raft_peer:new(Server, Log)}
      },

      NewState = State#state{inner_state = NewLeaderState, cluster = NewCluster},
      {keep_state,
       handle_command(From, {?CLUSTER_CHANGE, NewCluster}, NewState),
       [?ELECTION_TIMEOUT(get_min_timeout())]};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;
handle_event({call, From}, {leave, Server}, leader,
             #state{cluster = Cluster,
                    inner_state = #leader_state{peers = Peers} = LeaderState} = State) ->
  case raft_cluster:leave(Server, Cluster) of
    {ok, NewCluster} ->

      NewLeaderState = LeaderState#leader_state{
          peers = maps:remove(Server, Peers)
      },

      NewState = State#state{inner_state = NewLeaderState, cluster = NewCluster},
      {keep_state,
       handle_command(From, {?CLUSTER_CHANGE, NewCluster}, NewState),
       [?ELECTION_TIMEOUT(get_min_timeout())]};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;
handle_event(info, #append_entries_resp{term = Term}, leader,
        #state{current_term = CurrentTerm} = State)
    when CurrentTerm < Term ->
    NewState = State#state{current_term = Term,
                           voted_for = undefined},
    {next_state, follower, NewState};
handle_event(info, #append_entries_resp{} = AppendEntriesReq, leader, State) ->
    {keep_state, handle_append_entries(AppendEntriesReq, State)};

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

% not leader, can be dropped (check for newer term)
handle_event(info, #append_entries_resp{term = Term}, StateName,
             #state{current_term = CurrentTerm} = State) when StateName =/= leader andalso
    CurrentTerm < Term ->
    {next_state, follower, State#state{current_term = Term, voted_for = undefined}};
handle_event(info, #append_entries_resp{}, StateName, _State) when StateName =/= leader ->
    keep_state_and_data;
handle_event(info, #vote_req_reply{}, _StateName, _State) ->
  keep_state_and_data;

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
  send_append_reqs(State).

send_append_reqs(#state{log = Log, inner_state = #leader_state{peers = Peers}}) ->
  raft_peer:send_append_reqs(Peers, Log),
  ok.

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

execute({?CLUSTER_CHANGE, _NewCluster}, State) ->
  {reply, ok, State};
execute(Command, #state{callback = CallbackModule, user_state = UserState} = State) ->
  logger:debug("Execute log command ~p", [Command]),
  case apply(CallbackModule, execute, [Command, UserState]) of
    {reply, Reply, NewUserState} ->
      {reply, Reply, State#state{user_state = NewUserState}}
  end.

apply_cluster_change(NewCluster, #state{cluster = CurrentCluster} = State) ->
  NewClusterMembers = raft_cluster:members(NewCluster),
  logger:info("Appling new cluster config ~p", [NewCluster]),
  case lists:member(self(), NewClusterMembers) of
    true ->
      % @TODO unlink removed node?! or just ignore them when they die?
      [link(C) || C <- NewClusterMembers, C =:= self()],
      case raft_cluster:leader(NewCluster) =:= raft_cluster:leader(CurrentCluster) of
        true ->
          {keep_state, State#state{cluster = NewCluster}};
        _ ->
          {next_state, follower, State#state{cluster = NewCluster}}
      end;
    false ->
      % was new cluster leader member before out cluster? if not just ignore.
      case lists:member(raft_cluster:leader(NewCluster), raft_cluster:members(CurrentCluster)) of
        true ->
          logger:info("Leaving the cluster", []),
          {stop, leaving_cluster, State#state{cluster = raft_cluster:new()}};
        false ->
          logger:info("Ignoring cluster changes ~p", [NewCluster]),
          {keep_state, State}
      end
  end.


send_msg(Target, Msg) ->
  erlang:send_nosuspend(Target, Msg).

maybe_step_down_to_follower(#append_entries_req{term = Term, leader = Leader},
                            #state{current_term = CurrentTerm, cluster = Cluster} = State)
  when Term > CurrentTerm ->
  {next_state, follower, State#state{current_term = Term,
                                     voted_for = undefined,
                                     cluster = raft_cluster:leader(Cluster, Leader)}};
maybe_step_down_to_follower(_, State) ->
  {keep_state, State}.

maybe_delete_log(Log, #append_entries_req{prev_log_index = PrevLogIndex, term = AppendTerm}) ->
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {LogTerm, _Command}} when AppendTerm =/= LogTerm ->
      % 3. If an existing entry conflicts with a new one (same index
      % but different terms), delete the existing entry and all that follow it (§5.3)
      raft_log:delete(Log, PrevLogIndex);
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
  CurrentIndex = LastApplied+1,
  {ok, {_Term, Command}} = raft_log:get(Log, CurrentIndex),
  {reply, _, NewState} = execute(Command, State),
  maybe_apply(NewState#state{last_applied = CurrentIndex});
maybe_apply(State) ->
    {ok, State}.

has_majority(#active_request{majority = Majority, replicated = Replicated})
    when Majority >= length(Replicated) ->
    true;
has_majority(_) ->
    false.

handle_command(From, Command, #state{log = Log, current_term = Term, cluster = Cluster,
                                     inner_state = #leader_state{active_requests = CurrentRequests} = LeaderState} = State) ->
  logger:debug("Handling command: ~p", [Command]),
  print_record(State),

  NewLog = raft_log:append(Log, Command, Term),
  LogIndex = raft_log:last_index(NewLog),
  NewState = State#state{log = NewLog, committed_index = LogIndex},
  send_append_reqs(NewState),

  ActiveRequest = #active_request{log_index = LogIndex,
                                  from = From,
                                  majority = raft_cluster:majority_count(Cluster)},
  NewState2 = NewState#state{inner_state =
                   LeaderState#leader_state{
                       active_requests = CurrentRequests#{LogIndex => ActiveRequest}
                 }
    },

  SelfAppendEntriesResp = #append_entries_resp{term = Term,
                                               server = self(),
                                               match_index = LogIndex,
                                               success = true},

  handle_append_entries(SelfAppendEntriesResp, NewState2).

handle_append_entries(#append_entries_resp{term = _Term,
                                           success = true,
                                           server = Server,
                                           match_index = MatchIndex} = AppendEntriesResp,
                     #state{inner_state = #leader_state{active_requests = ActiveRequests,
                                                        peers = Peers} = InnerState,
                            log = Log
                     } = State) ->
    print_record(AppendEntriesResp),
  logger:debug("Got positive append_entries_resp from ~p with match_index ~p",
               [Server, MatchIndex]),
    Self = self(),
    NewInnerState =
    case Server of
      Self ->
        InnerState;
      _ ->
        Peer = maps:get(Server, Peers),
        NewPeer = raft_peer:match_index(Peer, MatchIndex),
        case raft_peer:has_more_to_replicate(NewPeer) of
          true ->
            raft_peer:send_append_req(NewPeer, Log);
          _ ->
            ok
        end,
        InnerState#leader_state{peers = Peers#{Server => NewPeer}}
    end,

    case maps:get(MatchIndex, ActiveRequests, no_request) of
        no_request ->
          logger:debug("No active request found for ~p index",  [MatchIndex]),
          State#state{inner_state = NewInnerState};
        #active_request{replicated = Replicated} = ActiveRequest ->
          logger:debug("Activer request found for ~p", [MatchIndex]),
          NewReplicated =
            case lists:member(Server, Replicated) of
                true ->
                    % repeated msg, no add
                    Replicated;
                _ ->
                    [Server | Replicated]
            end,
            NewActiveRequest = ActiveRequest#active_request{replicated = NewReplicated},
            case has_majority(NewActiveRequest) of
                true ->
                  logger:debug("Majority of the logs are successfully replicated for ~p",
                               [MatchIndex]),
                    {ok, {_Term, Command}} = raft_log:get(State#state.log, MatchIndex),
                    {reply, Reply, NewState} = execute(Command, State),
                    gen_statem:reply(NewActiveRequest#active_request.from, Reply),
                    NewActiveRequests = maps:remove(MatchIndex, ActiveRequests),
                    NewState#state{inner_state = NewInnerState#leader_state{active_requests =
                                                                            NewActiveRequests},
                                   last_applied = MatchIndex};
                _ ->
                  logger:debug("No majority has reached yet for log ~p", [MatchIndex]),
                    NewActiveRequests = ActiveRequests#{MatchIndex => NewActiveRequest},
                    State#state{inner_state = NewInnerState#leader_state{active_requests = NewActiveRequests}}
            end
    end;
handle_append_entries(#append_entries_resp{term = _Term,
                                           success = false,
                                           server = Server},
                      #state{inner_state = #leader_state{peers = Peers} = InnerState,
                             log = Log
                      } = State) ->
    % update match_index
    Peer = maps:get(Server, Peers),
    NewPeer = raft_peer:decrease_match_index(Peer),
    % @TODO remove sleep
    timer:sleep(100),
    raft_peer:send_append_req(NewPeer, Log),

   logger:warning("Got negative appen_entries_resp", []),
    Peer = maps:get(Server, Peers),
    State#state{inner_state = InnerState#leader_state{peers = Peers#{Server => NewPeer}}}.

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
  ?LOG(Msg, [self(), Term, Leader, PLI, PLT, Entries, LCI]);
print_record(#append_entries_resp{term = Term,
                                 server = Server,
                                 match_index = MatchIndex,
                                 success = Success}) ->
    Msg = "**** Append entries resp [~p] ****~n" ++
          "term = ~p~n" ++
          "server = ~p~n" ++
          "match_index = ~p~n" ++
          "success = ~p~n ~n",
    ?LOG(Msg, [self(), Term, Server, MatchIndex, Success]);
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
  ?LOG(Msg, [self(), CurrentTerm, raft_cluster:leader(Cluster), VotedFor,
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