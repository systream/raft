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
-define(MAX_HEARTBEAT_TIMEOUT, 35000).
-define(MIN_HEARTBEAT_TIMEOUT, 15000).
-define(HEARTBEAT_GRACE_TIME, 5000).

-define(ELECTION_TIMEOUT(Timeout), {state_timeout, Timeout, election_timeout}).
-define(ELECTION_TIMEOUT, ?ELECTION_TIMEOUT(get_timeout())).

-define(PRE_CLUSTER_CHANGE, '$raft_pre_cluster_change').
-define(CLUSTER_CHANGE, '$raft_cluster_change').

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

-record(follower_state, {
  %committed_index :: log_index()
}).

-record(candidate_state, {
  majority :: pos_integer(),
  granted = 0 :: non_neg_integer()
}).

-record(leader_state, {
  next_index = #{} :: #{pid() => log_index()},
  match_index = #{} :: #{pid() => log_index()}
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

  leader :: pid() | undefined,

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
  ?LOG("[~p] starting ~p~n", [self(), CallbackModule]),
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
  case raft_cluster:majority_count(Cluster) of
    1 ->
      % Alone in cluster, no one will send heartbeat, so step up for candidate immediately
      ?LOG("[~p, follower] start follower~n", [self()]),
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT(0)]};
    _ ->
      ?LOG("[~p, follower] become follower~n", [self()]),
      {keep_state, State#state{inner_state = #follower_state{}}, [?ELECTION_TIMEOUT]}
  end;
handle_event(state_timeout, election_timeout, follower, State) ->
  ?LOG("[~p, follower] heartbeat timeout~n", [self()]),
  {next_state, candidate, State};

%% %%%%%%%%%%%%%%%
%% Candidate
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, candidate,
             State = #state{current_term = Term, cluster = Cluster, log = Log}) ->
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
  erlang:send(Me, #vote_req_reply{term = NewTerm, granted = true
  }),
  ?LOG("[~p, candidate] Send vote request for term ~p ~n", [self(), Term]),
  send(Cluster, VoteReq),
  InnerState = #candidate_state{majority = raft_cluster:majority_count(Cluster)},
  NewState = State#state{current_term = NewTerm, inner_state = InnerState,
                         leader = undefined, voted_for = undefined},
  {keep_state, NewState, [?ELECTION_TIMEOUT]};

% process vote reply
handle_event(info, #vote_req_reply{term = Term, granted = true}, candidate,
             #state{current_term = Term,
                    inner_state = InnerState = #candidate_state{granted = Granted}} = State) ->
  NewInnerState = InnerState#candidate_state{granted = Granted+1},
  ?LOG("[~p, candidate] Got positve vote req reply~n", [self()]),
  case NewInnerState of
    #candidate_state{granted = G, majority = M} when G < M ->
      ?LOG("[~p, candidate] Not have the majority of the votes yet ~n", [self()]),
      {keep_state, State#state{inner_state = NewInnerState}};
    _ ->
      ?LOG("[~p, candidate] Have the majority of the votes ~n", [self()]),
      {next_state, leader, State#state{inner_state = NewInnerState}}
  end;
handle_event(info, #vote_req_reply{term = Term, granted = false}, candidate,
             State = #state{current_term = CurrentTerm}) when Term > CurrentTerm ->
  ?LOG("[~p, candidate] Vote req reply has a higher term ~n", [self()]),
  {next_state, follower, State#state{current_term = Term, voted_for = undefined}};
handle_event(info, #vote_req_reply{granted = false}, candidate, State) ->
  ?LOG("[~p, candidate] Got negative vote req reply ~n", [self()]),
  {keep_state, State};
handle_event(state_timeout, election_timeout, candidate, State) ->
  ?LOG("[~p, candidate] Election timeout ~p ~n", [self(), State]),
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
  ?LOG("[~p, ~p] Got append_entries_req with lower term ~p, send false ~n",
       [self(), _StateName, {Term, CurrentTerm}]),
  send_msg(Leader, #append_entries_resp{term = CurrentTerm, server = self(), success = false}),
  keep_state_and_data;
handle_event(info,
             #append_entries_req{prev_log_index = PrevLogIndex, leader = Leader, term = Term,
                                 leader_commit_index = LeaderCommitIndex} = AppendReq,
             _StateName,
             #state{log = Log, current_term = Term, committed_index = CI} = State) ->
  print_record(AppendReq),
  case raft_log:get(Log, PrevLogIndex) of
    {ok, {_Index, LogTerm, _Command}} when AppendReq#append_entries_req.term =/= LogTerm ->
      ?LOG("[~p, ~p] Got append_entries_req with different log term ~p, send false ~n",
           [self(), _StateName, {Term, LogTerm}]),
      send_msg(Leader, #append_entries_resp{term = Term, server = self(), success = false}),
      maybe_change_term(AppendReq, State);
    _ ->
      NewLog = append_commands(AppendReq, Log),
      ?LOG("[~p, ~p] Got append_entries_req ~p send true~n", [self(), _StateName, Term]),
      send_msg(Leader, #append_entries_resp{term = Term, server = self(), success = true}),
      ?LOG("[~p, ~p] maybe update ci ~p ~n", [self(), _StateName, {LeaderCommitIndex, CI,
                                                                   raft_log:last_index(NewLog)}]),
      NewCommitIndex = maybe_update_commit_index(LeaderCommitIndex, CI, NewLog),
      NewState = State#state{log = NewLog, current_term = Term, leader = Leader,
                             committed_index = NewCommitIndex},
      print_record(NewState),
      maybe_change_term(AppendReq, maybe_apply(NewState))
  end;
% handle vote requests
% If votedFor is null or candidateId, and candidate’s log is at
% least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm} = State) when Term < CurrentTerm ->
  ?LOG("[~p, ~p] Got vote req lower term thant current, reply negative ~p ~n",
       [self(), _StateName, Candidate]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false}),
  {keep_state, State};
handle_event(info, #vote_req{term = Term, candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm} = State) when Term > CurrentTerm ->
  ?LOG("[~p, ~p] Got vote req higher term thant current, reply positive ~p ~n",
       [self(), _StateName, Candidate]),
  send_msg(Candidate, #vote_req_reply{term = Term, granted = true}),
  {next_state, follower, State#state{current_term = Term, voted_for = Candidate}};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = undefined} = State) ->
  ?LOG("[~p, ~p] Got vote req not_voted yet, reply positive ~p ~n",
       [self(), _StateName, Candidate]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = true}),
  {keep_state, State#state{voted_for = Candidate}};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = Candidate} = State) ->
  ?LOG("[~p, ~p] Got vote req already voted for the same (~p), reply positive ~n",
       [self(), _StateName, Candidate]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = true}),
  {keep_state, State};
handle_event(info, #vote_req{candidate = Candidate}, _StateName,
             #state{current_term = CurrentTerm, voted_for = _OtherCandidate} = State) ->
  ?LOG("[~p, ~p] Got vote req already voted for the other candidate, reply negative ~p ~n",
       [self(), _StateName, Candidate]),
  send_msg(Candidate, #vote_req_reply{term = CurrentTerm, granted = false}),
  {keep_state, State};

handle_event({call, From}, status, StateName,
             #state{current_term = CurrentTerm, leader = Leader, cluster = Servers}) ->
  gen_statem:reply(From, {StateName, CurrentTerm, Leader, [raft_cluster:members(Servers)]}),
  keep_state_and_data;

%% %%%%%%%%%%%%%%%
%% Leader
%% %%%%%%%%%%%%%%%
handle_event(enter, _PrevStateName, leader, #state{cluster = Cluster, log = Log} = State) ->
  ?LOG("[~p, leader] become leader~n", [self()]),
  NextIndex = raft_log:next_index(Log),
  Members = raft_cluster:members(Cluster),

  LeaderState = #leader_state{
    next_index = [#{Member => NextIndex} || Member <- Members],
    match_index = [#{Member => 0} || Member <- Members]
  },

  NewState = State#state{leader = self(), inner_state = LeaderState},

  % Upon election: send initial empty AppendEntries RPCs
  % (heartbeat) to each server; repeat during idle periods to
  % prevent election timeouts (§5.2)
  send_heartbeat(NewState),
  {keep_state, NewState, [{state_timeout, get_min_timeout(), election_timeout}]};
handle_event(state_timeout, election_timeout, leader, State) ->
  send_heartbeat(State),
  {keep_state_and_data, [{state_timeout, get_min_timeout(), election_timeout}]};
handle_event({call, From}, {command, Command}, leader, State) ->
  {Result, NewState} = handle_command(Command, State),
  gen_statem:reply(From, Result),
  {keep_state, NewState};
handle_event({call, From}, {join, Server}, leader,
             #state{cluster = Cluster} = State) ->
  case raft_cluster:join(Server, Cluster) of
    {ok, NewCluster} ->
      {Reply, NewState2} = handle_command({?CLUSTER_CHANGE, NewCluster}, State),
      gen_statem:reply(From, Reply),
      {keep_state, NewState2};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;
handle_event({call, From}, {leave, Server}, leader, #state{cluster = Cluster} = State) ->
  case raft_cluster:leave(Server, Cluster) of
    {ok, NewCluster} ->
      {Reply, NewState2} = handle_command({?CLUSTER_CHANGE, NewCluster}, State),
      gen_statem:reply(From, Reply),
      {keep_state, NewState2};
    {error, _} = Error ->
      gen_statem:reply(From, Error),
      keep_state_and_data
  end;

%% %%%%%%%%%%%%%%%%%%%%%%%%%
%% Proxy commands to leader
%% %%%%%%%%%%%%%%%%%%%%%%%%%

handle_event({call, _From}, {command, Command}, _StateName,
             #state{leader = undefined}) ->
  ?LOG("[~p, ~p] postpone command no leader yet~p~n",
       [self(), _StateName, {command, Command}]),
  {keep_state_and_data, [postpone]};
handle_event({call, From}, {command, Command}, _StateName,
             #state{leader = Leader}) ->
  ?LOG("[~p, ~p] proxy req ~p~n", [self(), _StateName, {command, Command}]),
  send_msg(Leader, {proxy, {call, From}, {command, Command}}),
  keep_state_and_data;

handle_event({call, _From}, {Type, _Node}, _StateName,
             #state{leader = undefined}) when Type =:= join orelse Type =:= leave ->
  ?LOG("[~p, ~p] postpone Cluster change / no leader yet~p~n",
       [self(), _StateName, {Type, _Node}]),
  {keep_state_and_data, [postpone]};
handle_event({call, From}, {Type, Node}, _StateName,
             #state{leader = Leader}) when Type =:= join orelse Type =:= leave ->
  ?LOG("[~p, ~p] proxy req ~p~n", [self(), _StateName, {Type, Node}]),
  send_msg(Leader, {proxy, {call, From}, {Type, Node}}),
  keep_state_and_data;

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

% silence for unhandled now append_entries resp
handle_event(info, #append_entries_resp{}, _StateName, _State) ->
  keep_state_and_data;
handle_event(info, #vote_req_reply{}, _StateName, _State) ->
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
  print_record(State),
  send_append_req([], State).

send_append_req(Commands, #state{current_term = Term, log = Log,
                                 cluster = Cluster, committed_index = CI}) ->
  ?LOG("[~p] send append log entries ~p~n", [self(), raft_cluster:members(Cluster)]),
  AppendEntriesReq = #append_entries_req{term = Term,
                                         leader = self(),
                                         prev_log_index = raft_log:last_index(Log),
                                         prev_log_term = raft_log:last_term(Log),
                                         entries = Commands,
                                         leader_commit_index = CI},
  send(Cluster, AppendEntriesReq).

send(Cluster, Msg) ->
  [send_msg(Server, Msg) || Server <- raft_cluster:members(Cluster), Server =/= self()].

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

execute({?CLUSTER_CHANGE, NewCluster}, Index, State) ->
  NewClusterMembers = raft_cluster:members(NewCluster),
  case lists:member(self(), NewClusterMembers) of
    true ->
      ?LOG("[~p] Appling new cluster config ~p~n", [self(), NewCluster]),
      % @TODO unlink removed nodes?! or just ignore them when they die?
      [link(C) || C <- NewClusterMembers, C =:= self()],
      {reply, ok, State#state{cluster = NewCluster, last_applied = Index}};
    false ->
      ?LOG("[~p] Leaving the cluster~n", [self()]),
      % not in new cluster so need to be alone
      {reply, ok, State#state{cluster = raft_cluster:new(), last_applied = Index}}
  end;
execute(Command, Index, #state{callback = CallbackModule, user_state = UserState} = State) ->
  ?LOG("[~p] execute log command ~p~n", [self(), Command]),
  case apply(CallbackModule, execute, [Command, UserState]) of
    {reply, Reply, NewUserState} ->
      {reply, Reply, State#state{user_state = NewUserState,
                                 last_applied = Index}}
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
  {ok, {Index, _Term, Command}} = raft_log:get(Log, raft_log:next_index(Log)),
  {reply, _, NewState} = execute(Command, Index, State),
  maybe_apply(NewState);
maybe_apply(State) ->
  State.

wait_for_append_resp(Term, Cluster) ->
  wait_for_append_resp(Term, raft_cluster:majority_count(Cluster), {0, 0}).

wait_for_append_resp(_Term, Majority, {Pos, _Neg}) when Majority >= Pos ->
  replicated;
wait_for_append_resp(_Term, Majority, {_Pos, Neg}) when Majority >= Neg ->
  rejected;
wait_for_append_resp(Term, Majority, {Pos, Neg}) ->
  receive
    #append_entries_resp{term = Term, success = true} ->
      wait_for_append_resp(Term, Majority, {Pos+1, Neg});
    #append_entries_resp{term = Term, success = false} ->
      wait_for_append_resp(Term, Majority, {Pos, Neg+1})
  % @ TODO monitor processes -> remove here
  after 3000 ->
    timeout
  end.

handle_command(Command, #state{log = Log, current_term = Term, cluster = Cluster} = State) ->
  ?LOG("[~p, leader] command ~p~n", [self(),Command]),
  NewLog = raft_log:append(Log, Command, Term),

  % send with old state (old log ref) to get last_term/last_index to previous
  ?LOG("[~p, leader] sending append entries to ~p~n", [self(), raft_cluster:members(Cluster)]),
  send_append_req([Command], State),

  % @TODO: majority commit!?
  ?LOG("[~p, leader] waiting replies~n", [self()]),
  case wait_for_append_resp(Term, Cluster) of
    replicated ->
      ?LOG("[~p, leader] logs are successfully replicated~n", [self()]),
      NewState = State#state{log = NewLog, committed_index = raft_log:last_index(NewLog)},
      {reply, Reply, NewState2} = execute(Command, raft_log:last_index(NewLog), NewState),
      % @TODO: this is not needed indeed
      send_heartbeat(NewState2),
      {Reply, NewState2};
    rejected ->
      ?LOG("[~p, leader] logs are UNsuccessfully replicated~n", [self()]),
      {{error, rejected}, State};
    timeout ->
      ?LOG("[~p, leader] logs are UNsuccessfully replicated, due to timeout~n", [self()]),
      {{error, timeout}, State}
  end.

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
  io:format(user, Msg, [self(), Term, Leader, PLI, PLT, Entries, LCI]);

%%  voted_for :: pid() | undefined,
%%  log :: raft_log:log_ref(),
%%
%%  leader :: pid() | undefined,
%%
%%  committed_index = 0 :: log_index(),
%%  last_applied = 0 :: log_index(),
%%
%%  inner_state :: follower_state() | candidate_state() | leader_state() | undefined

print_record(#state{current_term = CurrentTerm,
                    cluster = Cluster,
                    voted_for = VotedFor,
                    log = Log,
                    leader = Leader,
                    committed_index = CommittedIndex,
                    last_applied = LastApplied}) ->
  Msg = "**** State [~p] ****~n" ++
        "term = ~p~n" ++
        "leader = ~p~n" ++
        "voted_for = ~p~n" ++
        "cluster members = ~p~n" ++
        "committed_index = ~p~n" ++
        "last_applied = ~p~n" ++
        "log pos = ~p~n ~n",
  io:format(user, Msg, [self(), CurrentTerm, Leader, VotedFor, raft_cluster:members(Cluster),
                        CommittedIndex, LastApplied, {raft_log:last_index(Log),
                                                      raft_log:last_term(Log)}]).