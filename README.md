# Raft

Implementation of [replicated state machine (raft)](https://raft.github.io/) protocol.

Implemented:
* Leader election
* Log replication
* Cluster changes
* Log compaction (snapshotting)
* Request repetition filtering (bloom filter)

## Usage

Before starting a new raft server, you need to implement
`raft_server` behavior. 

 * `init/0`: return with the new staring state
 * `handle_command/2`: execute commands that can modify state
 * `handle_query/2`: execute queries which cannot change state

### Start new raft server
```erlang
{ok, Pid} = raft:start(callback_module).
```

### Commands
```erlang
raft:command(Pid, {do_stuff, Stuff}).
```

### Query
```erlang
raft:query(Pid, {get_stuff, Stuff}).
```

## Cluster changes

### Join new member to the cluster
```erlang
raft:join(PidA, PidB).
```

### Leave member
```erlang
raft:leave(PidA, PidB).
```

### Get status info

```erlang
#{role => leader | follower | candidate,
  term => 1,
  leader => <0.261.0>,
  cluster_members => [<0.261.0>, <0.253.0>, <0.243.0>],
  log_last_index => 1923,
  committed_index => 1922,
  last_applied => 1922,
  log => Last10LogEntry
} = raft:status(Pid)
```


## Config parameters

### Timeouts

#### Normal heartbeat timeout
How many times the followers wait for the heartbeat.

`max_heartbeat_timeout` - rand(`heartbeat_grace_time`) + `heartbeat_grace_time`

#### Consensus timeout
When electing a new leader candidates wait until this amount of time, and starts a new 
election.

`max_heartbeat_timeout` - rand(`heartbeat_grace_time`)

#### Leader timeout
Leader sending heartbeats (empty append_entries_req) if nothing sent to the followers for a while.

`min_heartbeat_timeout` for one heartbeat, than change to idle timeout until the next command

#### Idle timeout
After one period of heartbeat leader change it's heartbeat time to this. 
(`max_heartbeat_timeout`-rand(`heartbeat_grace_time`))

### Append entries chunk size

`max_append_entries_chunk_size`If a follower wants to catch up, leader can send more than one command in one message.
This parameters configures the max amount of commands send in one message. default: 100

### Automatic snapshot chunks size

`snapshot_chunk_size` After a certain amount of commands 
every raft server does log compactions (create a snapshot). default: 100000   

Build
-----

***WARNING***: Not yet prod ready

    $ rebar3 compile
