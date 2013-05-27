require 'delegate'
require 'raft/latch'
require 'raft/node'
require 'raft/peer'

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
      if command == :vote_request
        handle_vote_request(payload)
      elsif command == :append_entries
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
      info("[RPC] Received AppendEntries: #{payload}")
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
        if node.log.validate(payload[:prev_log_index], payload[:prev_log_term])
          node.log.append(payload[:entries])
        else
          false
        end
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

      super
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

      peers.map do |peer|
        async.request_vote(peer, payload)
      end

      nil
    end

    def request_vote(peer, payload)
      response = peer.request_vote(payload)

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
    attr_accessor :pacemakers
    attr_accessor :indices
    attr_accessor :commit_index

    def enter_state
      self.pacemakers = {}
      self.indices = {}

      peers.each do |peer|
        indices[peer] = log.last_index
        pacemakers[peer] = every(broadcast_time) { synchronize(peer) }
      end
    end

    def exit_state
      pacemakers.each_value(&:cancel)
    end

    def handle_append_entries(message)
      debug 'Ouch, I shouldnt receive that RPC.'
      raise 'Ouch, I shouldnt receive that RPC.'
    end

    def handle_vote_request(message)
      if message[:term] > node.term
        switch_state(:follower)
      end

      super
    end

    def execute(*commands)
      range = log.append(commands.map { |command| Raft::Log::Entry.new(command: command, term: term) })
      peers.each { |peer| synchronize(peer) }
    end

    def synchronize(peer)
      prev_index = indices[peer]

      if prev_index
        prev_term  = log[prev_index].term
        curr_index = prev_index + 1
        last_index = log.last_index
        entries = log[curr_index..last_index]
      else
        prev_term = nil
        entries = log[0..-1]
        last_index = log.last_index
      end

      payload = {
        term: term,
        prev_log_index: prev_index,
        prev_log_term: prev_term,
        commit_index: commit_index,
        entries: entries
      }

      response = peer.append_entries(payload)

      pacemakers[peer].reset

      if response[:success]
        indices[peer] = last_index
      else
        indices[peer] -= 1 if indices[peer] && indices[peer] > 0
      end
    end
  end
end
