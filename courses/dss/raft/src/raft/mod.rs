use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::{thread, time};

use futures::channel::mpsc::UnboundedSender;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Debug, Clone)]
pub enum ApplyMsg {
    NoOp,
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub role: Role,
    pub term: u64,
    pub voted_for: Option<usize>,
    pub log: Vec<(u64, ApplyMsg)>,

    // volatile state
    pub last_heartbeat_time: Option<time::Instant>,
    pub commit_index: usize,
    pub last_applied: usize,

    // volatile-leader only state
    /// index of the next log entry to send for each server.
    /// initialized to leader last log index + 1.
    pub next_index: Vec<usize>,
    /// index of the highest known log entry replicated for each server.
    /// initialized to be 0.
    pub match_index: Vec<usize>,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn to_follower(&mut self) {
        self.role = Role::Follower;
        self.voted_for = None;
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Mutex<Box<dyn Persister>>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<Mutex<State>>,
}

const FOLLOWER_TIMEOUT: u64 = 200;
const RPC_TIMEOUT: u64 = 50;

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister: Mutex::new(persister),
            me,
            state: Arc::new(Mutex::new(State::default())),
        };
        {
            let mut state = rf.state.lock().unwrap();
            // set NoOp as the first index to make all indexes start at 1.
            state.log.push((0, ApplyMsg::NoOp));
        }

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    fn start_event_loop(&self) {
        loop {
            let role = { self.state.lock().unwrap().role.clone() };
            match role {
                Role::Follower => self.start_follower_loop(),
                Role::Candidate => self.commence_election(),
                Role::Leader => self.start_leader_loop(),
            }
        }
    }

    // loop while in follower state
    // check if there has been a message
    fn start_follower_loop(&self) {
        info!("{}: starting follower loop", self.me);
        loop {
            let current_time = time::Instant::now();
            let rng = rand::thread_rng().gen_range(50, 100);
            let sleep_dur = FOLLOWER_TIMEOUT + rng;
            thread::sleep(time::Duration::from_millis(sleep_dur));
            let mut state = self.state.lock().unwrap();
            match state.role {
                Role::Candidate | Role::Leader => break,
                _ => (),
            }
            info!("{}: still follower - checking heartbeat timeout", self.me);
            match state.last_heartbeat_time {
                Some(t) if t > current_time => {
                    info!("{}: staying as follower", self.me);
                    continue; // received message after the previous sleep
                }
                _ => {
                    // revert to candidate state
                    info!("{}: election timeout - transitioning to candidate", self.me);
                    state.role = Role::Candidate;
                    break;
                }
            }
        }
    }

    fn commence_election(&self) {
        info!("{}: commencing election", self.me);
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        let (term, last_log_index, last_log_term) = {
            let mut state = self.state.lock().unwrap();
            state.term += 1;
            state.voted_for = Some(self.me);
            (
                state.term,
                state.log.len() as i64,
                state.log.last().map(|l| l.0).unwrap_or(0) as i64,
            )
        };
        for (i, _) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }
            let rx = self.send_request_vote(
                i,
                RequestVoteArgs {
                    term: term as i64,
                    candidate_id: self.me as i64,
                    last_log_index,
                    last_log_term,
                },
            );
            let tx = tx.clone();
            thread::spawn(move || {
                match rx.recv_timeout(time::Duration::from_millis(RPC_TIMEOUT)) {
                    Ok(res) => {
                        let _ = tx.send(res);
                    }
                    Err(_) => (),
                }
            });
        }
        info!("{}: sent request votes", self.me);
        let mut votes = 1; // 1 vote by self
        let required_votes = (self.peers.len() / 2) + 1;
        let responses = Raft::collect_responses(
            rx,
            Duration::from_millis(RPC_TIMEOUT),
            self.peers.len() - 1,
        );
        for res in responses {
            match res {
                Ok(res) => {
                    if res.voted {
                        info!("{}: received vote", self.me);
                        votes += 1;
                    } else {
                        info!(
                            "{}: did not receive vote - current term: {}",
                            self.me, res.term
                        );
                        let mut state = self.state.lock().unwrap();
                        if state.term < res.term as u64 {
                            info!(
                                "{}: received response with higher term, reverting to follower",
                                self.me
                            );
                            state.term = res.term as u64;
                            state.to_follower();
                            break;
                        }
                    }
                }
                Err(err) => error!("{}: RPC error: {}", self.me, err),
            }
        }

        // ensure still candidate
        let mut state = self.state.lock().unwrap();
        if Role::Candidate != state.role {
            info!("{}: no longer a candidate - ending election", self.me);
            return;
        }

        if votes >= required_votes {
            state.role = Role::Leader;
            info!("{}: became leader", self.me);
        } else {
            state.role = Role::Candidate;
            info!("{}: became candidate", self.me);
        }
    }

    fn start_leader_loop(&self) {
        info!("{}: became leader and start sending heartbeats", self.me);
        loop {
            let (role, term) = {
                let state = self.state.lock().unwrap();
                (state.role, state.term)
            };
            if role != Role::Leader {
                break;
            }
            let (tx, rx) = channel::<Result<AppendEntriesReply>>();
            for (i, _) in self.peers.iter().enumerate() {
                if i == self.me {
                    continue;
                }
                let rx = self.send_append_entries(
                    i,
                    AppendEntriesArgs {
                        term: term as i64,
                        leader_id: self.me as i64,
                    },
                );
                let tx = tx.clone();
                thread::spawn(move || {
                    match rx.recv_timeout(time::Duration::from_millis(RPC_TIMEOUT)) {
                        Ok(res) => {
                            let _ = tx.send(res);
                        }
                        Err(_) => (),
                    }
                });
            }

            // collect responses
            let responses = Raft::collect_responses(
                rx,
                Duration::from_millis(RPC_TIMEOUT),
                self.peers.len() - 1,
            );
            let mut state = self.state.lock().unwrap();
            for res in responses {
                if let Ok(res) = res {
                    if res.term as u64 > state.term {
                        info!(
                            "{}: received response with greater term, reverting to follower",
                            self.me
                        );
                        state.term = res.term as u64;
                        state.to_follower();
                    }
                }
            }
        }
    }

    fn collect_responses<T>(rx: Receiver<T>, timeout_dur: Duration, n: usize) -> Vec<T> {
        let mut responses = Vec::with_capacity(n);
        let deadline = time::Instant::now() + timeout_dur;
        let start_time = Instant::now();
        loop {
            let now = Instant::now();
            if deadline > now {
                let timeout = start_time + timeout_dur - now;
                match rx.recv_timeout(timeout) {
                    Ok(resp) => {
                        responses.push(resp);
                    }
                    Err(_) => break,
                }
            } else {
                break;
            }
        }

        responses
    }

    fn handle_request_vote(&self, req: RequestVoteArgs) -> RequestVoteReply {
        info!("{}: handle request vote from {}", self.me, req.candidate_id);
        let mut state = self.state.lock().unwrap();
        let current_term = state.term as i64;
        // figure 2 - all servers
        if req.term > current_term {
            info!(
                "{}: received RPC with higher term, updating own term and convert to follower",
                self.me
            );
            state.term = req.term as u64;
            state.to_follower();
        }

        let voted = if req.term < current_term {
            info!(
                "{}: rejecting vote to {} as term is lower",
                self.me, req.candidate_id
            );
            false
        } else if state.voted_for.is_some() {
            info!(
                "{}: rejecting vote to {} as already voted for another candiate",
                self.me, req.candidate_id
            );
            false
        } else {
            let last_term = state.log.last().unwrap().0 as i64;
            // up to date is determined by:
            // 1. if the logs have last entries with different terms, the log with the later term is more up to date
            // 2. if the logs have last entries with same terms, the longer log is more to up to date.
            !(req.last_log_term < last_term
                || (req.last_log_term == last_term && req.last_log_index < state.log.len() as i64))
        };
        if voted {
            state.voted_for = Some(req.candidate_id as usize);
            info!("{}: voted for {}", self.me, req.candidate_id);
        } else {
            info!("{}: did not vote for {}", self.me, req.candidate_id);
        }

        RequestVoteReply {
            term: current_term,
            voted,
        }
    }

    fn handle_append_entries(&self, req: AppendEntriesArgs) -> AppendEntriesReply {
        info!("{}: handle append entries from {}", self.me, req.leader_id);
        let mut state = self.state.lock().unwrap();
        let req_term = req.term as u64;
        if req_term > state.term {
            state.term = req_term;
            state.to_follower();
        }

        if req_term < state.term {
            info!("{}: rejecting append entries req", self.me);
            return AppendEntriesReply {
                term: state.term as i64,
                success: false,
            };
        }
        // TODO: if log does not contain entry at prevLogIndex that matches prevLogTerm
        // TODO: if conflicting entry exists
        // TODO: append new entries
        // TODO: update commit index

        state.last_heartbeat_time = Some(time::Instant::now());
        AppendEntriesReply {
            term: state.term as i64,
            success: true,
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        info!("{}: send request vote to {}", self.me, server);
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        info!("{}: sent append entries heartbeat to {}", self.me, server);
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        let _ = self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Raft>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let rf = Arc::new(raft);
        let clone = rf.clone();
        thread::spawn(move || {
            clone.start_event_loop();
        });
        Self { raft: rf }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.to_owned().state.to_owned().lock().unwrap().term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.to_owned().state.to_owned().lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let rf = self.raft.to_owned();
        let state = rf.state.lock().unwrap().clone();
        state
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        Ok(self.raft.handle_request_vote(args))
    }

    async fn append_entries(&self, req: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        Ok(self.raft.handle_append_entries(req))
    }
}
