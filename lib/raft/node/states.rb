require 'delegate'
require 'raft/latch'
require 'raft/node'

module Raft::Node::State
  class Base < Delegator
    include Celluloid::Logger

    attr_accessor :node

    # @param [Raft::Node] node
    def initialize(node)
      self.node = node
    end

    def __getobj__
      node
    end

    # Delegates to the node for convenience.
    # @see Raft::Node#switch_state
    def switch_state(state)
      node.switch_state(state)
    end

    # Called when entering this state.
    def enter_state
    end

    # Called on transition to another state.
    def exit_state
    end

    def handle_rpc(command, payload)
      info("Received RPC: #{command} #{payload}")

      if command == 'vote_request'
        handle_vote_request(payload)
      elsif command == 'append_entries'
        handle_append_entries(payload)
      else
        {error: 'Unknown command.'}
      end
    end

    # @param [Hash] request
    # @option message [Fixnum] :term            The candidate's term.
    # @option message [String] :candidate_id    The candidate requesting the vote.
    # @option message [Fixnum] :last_log_index  Index of the candidate's last log entry.
    # @option message [Fixnum] :last_log_term   Term of the candidate's last log entry.
    #
    # @return [Hash] response
    # @option response [Boolean] :vote_granted  Whether the candidate's receives the vote.
    # @option response [Fixnum]  :term          This node's current term.
    def handle_vote_request(request)
      term = request[:term]
      candidate_id = request[:candidate_id]

      unless node.validate_term(term)
        return {term: node.term, vote_granted: false}
      end

      valid_candidate = (node.voted_for.nil? || node.voted_for == candidate_id)
      log_complete = node.log.complete?(request[:last_log_term], request[:last_log_index])

      vote_granted = (valid_candidate && log_complete)

      node.voted_for = candidate_id if vote_granted
      return {term: node.term, vote_granted: vote_granted}
    end

    def handle_append_entries(payload)
      term = payload[:term]

      unless node.validate_term(term)
        return {term: node.term, success: false}
      end

      # TODO: This should go into the subclasses.
      if is_a?(Candidate) || is_a?(Leader)
        switch_state(:follower)
      end

      timeout.reset

      success = if payload[:entries].any?
        node.log.append(payload[:prev_log_index], payload[:prev_log_term], payload[:entries])
      else
        true
      end

      return {term: node.term, success: success}
      # TODO 8.! Apply newly committed entries to state machine (§5.3)
    end
  end

  class Follower < Base
    attr_accessor :timeout

    def enter_state
      self.timeout = node.after(node.random_timeout, &method(:on_timeout))
    end

    # Reset timeout when granting vote to candidate.
    def handle_vote_request(payload)
      response = super(payload)
      timeout.reset if response[:vote_granted]
      response
    end
    
    # Reset timeout when receiving valid AppendEntries RPC.
    # FIXME: This is pseudo-code.
    def handle_append_entries(payload)
      response = super(payload)

      if response[:success]
        timeout.reset
      end

      response
    end

    def on_timeout
      node.enter_new_term
      switch_state(:candidate)
    end
  end

  class Candidate < Base
    attr_accessor :timeout
    attr_accessor :election

    # @return [Fixnum] Number of received votes.
    attr_accessor :votes

    def enter_state
      self.election = Raft::Latch.new
      self.votes = 0

      set_timeout
      collect_votes

      async.wait_for_election
    end

    def wait_for_election
      begin
        result, value = election.wait
        self.timeout.cancel if timeout

        case result
        when :success
          switch_state(:leader)
        when :new_leader_detected
          switch_state(:follower)
        when :higher_term_detected
          node.enter_new_term(value)
          switch_state(:follower)
        when :timeout
          node.enter_new_term(value)
          switch_state(:candidate)
        end
      ensure
        self.timeout.cancel if timeout
      end
    end

    def set_timeout
      unless timeout
        self.timeout = Celluloid.after(node.random_timeout*2) { election.signal(:timeout) }
      else
        timeout.reset
      end
    end

    def handle_append_entries(payload)
      election.signal(:new_leader_detected)
    end

    def handle_vote_request(message)
      if message[:term] > node.term
        node.enter_new_term(message[:term])
        switch_state(:follower)
      end

      super
    end

    def collect_votes
      payload = {
        term: node.term,
        last_log_term: node.log.last_term,
        last_log_index: node.log.last_index,
        candidate_id: node.id
      }

      node.options[:peers].map do |peer|
        async.request_vote(peer, payload)
      end

      nil
    end

    def request_vote(peer, payload)
      node.debug('[RPC] Requesting vote from ' + peer)
      response = node.client.vote_request(peer, payload)
      node.debug("[RPC] Response from peer #{peer}: #{response.inspect}")

      # Step down when a higher term is detected.
      # Accept votes from peers in the same term.
      # Ignore votes from peers with an older term.
      if response[:term] > node.term
        election.signal(:higher_term_detected, response[:term])
      elsif response[:term] == node.term
        self.votes += 1 if response[:vote_granted]

        # Is this node winning?
        if votes >= node.cluster_quorum
          election.signal(:success, votes) unless election.ready?
        end
      end
    end
  end

  class Leader < Base
    # @return [Timers::Timer]
    attr_accessor :heartbeat

    # A map holding peers and their next index (Section 5.3).
    #
    # > The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will
    # > send to that follower. 
    attr_accessor :peer_indices

    # @return [Fixnum] Index of the last entry known to be committed.
    attr_accessor :commit_index

    def enter_state
      # Send periodic heartbeats to all peers.
      self.heartbeat = node.after(node.broadcast_time) do
        send_heartbeat
        heartbeat.reset
      end

      # When a leader first comes to power it initializes all nextIndex values to the index just after the last one in
      # its log (Section 5.3).
      self.peer_indices = peers.each_with_object(Hash.new) { |peer, hash| hash[peer] = log.last_index + 1 }
    end

    def exit_state
      heartbeat.cancel
    end

    def send_heartbeat
      payload = {
        term: node.term,
        leader_id: node.id,
        prev_log_index: log.last_index,
        prev_log_term: log.last_term,
        entries: [],
        commit_index: commit_index
      }

      client.multicall(peers, :append_entries, payload, node.broadcast_time)
    end

    def execute(*args)
      entry = Entry.new(command: args, term: term)
      log.append(entry)

      payload = {
        term: term,
        leader_id: id,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [entry],
        commit_index: 0
      }

      # Replicate the command to all peers until it can be considered committed:
      #
      # > a log entry may only be considered committed if the entry is stored on a majority of the servers; in addition,
      # > at least one entry from the leader's current term must also be stored on a majority of the servers (Section 5.4)
      until committed?
        client.multicall(:append_entries, payload, broadcast_time)
      end
      futures = peers.map { |peer| future.replicate_entry(entry, peer) }
      quorum_wait(futures)

      entry.committed = true

      # Now execute the command.
      entry.execute

      result
    end

    def replicate_entry(entry, peer)
      payload = {
        term: node.term,
        leader_id: node.id,
        prev_log_index: log.last_index,
        prev_log_term: log.last_term,
        entries: [entry],
        commit_index: commit_index
      }

      response = client.append_entry(peer, payload)

      return true if response[:success]
    end

    def quorum_wait(futures)
      condition = Condition.new
      results = []

      futures.each { |future| async.signal_when_done(condition, future) }

      until results >= cluster_quorum
        results << condition.wait
      end

      results
    end

    def signal_when_done(condition, future)
      condition.signal(future.value)
    end

    def handle_vote_request(message)
      if message[:term] > node.term
        switch_state(:follower)
      end

      super
    end

    def handle_append_entries(message)
    end
  end
end
