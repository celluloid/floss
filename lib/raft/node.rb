# encoding: utf-8

require 'raft/rpc/zmq'
require 'raft/log'
require 'raft/peer'
require 'raft/one_off_latch'
require 'raft/count_down_latch'

class Raft::Node
  include Celluloid
  include Celluloid::FSM
  include Celluloid::Logger

  state(:follower, default: true, to: :candidate)
  state(:candidate, to: [:leader, :follower]) do
    enter_new_term
    start_election
  end
  state(:leader, to: [:follower]) { info('I AM THE LEADER'); start_log_replication }

  # Default broadcast time.
  # @see #broadcast_time
  BROADCAST_TIME = 0.020

  # Default election timeout.
  # @see #election_timeout
  ELECTION_TIMEOUT = (0.150..0.300)

  # @return [Raft::Log] The replicated log.
  attr_accessor :log

  # @return [Raft::RPC::Server]
  attr_accessor :server

  DEFAULT_OPTIONS = {
    rpc: Raft::RPC::ZMQ,
    run: true
  }.freeze

  # @param [Hash] options
  # @option options [String] :id            A string identifying this node, often its RPC address.
  # @option options [Array<String>] :peers  Identifiers of all peers in the cluster.
  # @option options [Module,Class] :rpc     Namespace containing `Server` and `Client` classes.
  def initialize(options = {}, &handler)
    super

    @options = DEFAULT_OPTIONS.merge(options)
    @current_term = 0
    @ready_latch = Raft::OneOffLatch.new
    @running = false

    self.log = Raft::Log.new

    async.run if @options[:run]
  end

  def run
    raise 'Already running' if @running

    @running = true

    self.server = link(rpc_server_class.new(id, &method(:handle_rpc)))
    @election_timeout = after(random_timeout) { on_election_timeout }
  end

  # Blocks until the node is ready for executing commands.
  def wait_until_ready
    @ready_latch.wait
  end

  def rpc_server_class
    @options[:rpc].const_get('Server')
  end

  def rpc_client_class
    @options[:rpc].const_get('Client')
  end

  # Returns this node's id.
  # @return [String]
  def id
    @options[:id]
  end

  # Returns peers in the cluster.
  # @return [Array<Raft::Peer>]
  def peers
    @peers ||= @options[:peers].map { |peer| Raft::Peer.new(peer, rpc_client_class: rpc_client_class) }
  end

  # Returns the cluster's quorum.
  # @return [Fixnum]
  def cluster_quorum
    (cluster_size / 2) + 1
  end

  # Returns the number of nodes in the cluster.
  # @return [Fixnum]
  def cluster_size
    peers.size + 1
  end

  # The interval between heartbeats (in seconds). See Section 5.7.
  #
  # > The broadcast time must be an order of magnitude less than the election timeout so that leaders can reliably send
  # > the heartbeat messages required to keep followers from starting elections.
  #
  # @return [Float]
  def broadcast_time
    @options[:broadcast_time] || BROADCAST_TIME
  end

  # Randomized election timeout as defined in Section 5.2.
  #
  # This timeout is used in multiple ways:
  #
  #   * If a follower does not receive any activity, it starts a new election.
  #   * As a candidate, if the election does not resolve within this time, it is restarted.
  #
  # @return [Float]
  def random_timeout
    range = @options[:election_timeout] || ELECTION_TIMEOUT
    min, max = range.first, range.last
    min + rand(max - min)
  end

  def enter_new_term(new_term = nil)
    @current_term = (new_term || @current_term + 1)
    @voted_for = nil
  end

  %w(info debug warn error).each do |m|
    define_method(m) do |str|
      super("[#{id}] #{str}")
    end
  end

  states.each do |name, _|
    define_method(:"#{name}?") do
      # TODO: FIXME: VERIFY
      raise 'WTF' unless state.is_a?(Symbol)
      self.state == name
    end
  end

  def execute(*commands)
    range = log.append(commands.map { |command| Raft::Log::Entry.new(command: command, term: term) })
    peers.each { |peer| synchronize(peer) }
    range
  end

  def handle_rpc(command, payload)
    handler = :"handle_#{command}"

    if respond_to?(handler)
      send(handler, payload)
    else
      abort ArgumentError.new('Unknown command.')
    end
  end

  protected

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
    info("[RPC] Received VoteRequest: #{request}")

    term = request[:term]
    candidate_id = request[:candidate_id]

    if term < @current_term
      return {term: @current_term, vote_granted: false}
    end

    if term > @current_term
      enter_new_term(term)
      stop_log_replication if leader?
      transition(:follower) if candidate? || leader?
    end

    valid_candidate = @voted_for.nil? || @voted_for == candidate_id
    log_complete = log.complete?(request[:last_log_term], request[:last_log_index])

    vote_granted = (valid_candidate && log_complete)

    if vote_granted
      @voted_for = candidate_id
      @election_timeout.reset
    end

    return {term: @current_term, vote_granted: vote_granted}
  end

  def handle_append_entries(payload)
    info("[RPC] Received AppendEntries: #{payload}")

    # Marks the node as ready for accepting commands.
    @ready_latch.signal

    term = payload[:term]

    # Reject RPCs with a lesser term.
    if term < @current_term
      return {term: @current_term, success: false}
    end

    # Accept terms greater than the local one.
    if term > @current_term
      enter_new_term(term)
    end

    # Step down if another node sends a valid AppendEntries RPC.
    stop_log_replication if leader?
    transition(:follower) if candidate? || leader?

    # A valid AppendEntries RPC resets the election timeout.
    @election_timeout.reset

    success = if payload[:entries].any?
      if log.validate(payload[:prev_log_index], payload[:prev_log_term])
        log.append(payload[:entries])
      else
        false
      end
    else
      true
    end

    return {term: @current_term, success: success}
    # TODO 8.! Apply newly committed entries to state machine (§5.3)
  end

  def on_election_timeout
    if follower?
      transition(:candidate)
    end

    if candidate?
      enter_new_term
      transition(:candidate)
    end
  end

  # @group Candidate methods

  def start_election
    @votes = Raft::CountDownLatch.new(cluster_quorum)
    collect_votes

    @votes.wait

    transition(:leader)
  end

  def collect_votes
    payload = {
      term: @current_term,
      last_log_term: log.last_term,
      last_log_index: log.last_index,
      candidate_id: id
    }

    peers.each do |peer|
      async.request_vote(peer, payload)
    end
  end

  # TODO: The candidate should retry the RPC if a peer doesn't answer.
  def request_vote(peer, payload)
    response = peer.request_vote(payload)
    term = response[:term]

    # Ignore old responses.
    return if @current_term > term

    # Step down when a higher term is detected.
    # Accept votes from peers in the same term.
    # Ignore votes from peers with an older term.
    if @current_term < term
      enter_new_term(term)
      transition(:follower)

      return
    end

    @votes.signal if response[:vote_granted]
  end

  # @group Leader methods

  def start_log_replication
    @pacemakers = {}
    @indices = {}

    peers.each do |peer|
      @indices[peer] = log.last_index
      @pacemakers[peer] = every(broadcast_time) { replicate_log_to(peer) }
    end
  end

  def stop_log_replication
    @pacemakers.each(&:cancel)
    @pacemakers.clear
  end

  def replicate_log_to(peer)
    prev_index = @indices[peer]

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
      term: @current_term,
      prev_log_index: prev_index,
      prev_log_term: prev_term,
      commit_index: @commit_index,
      entries: entries
    }

    response = peer.append_entries(payload)

    @pacemakers[peer].reset

    if response[:success]
      @indices[peer] = last_index
    else
      @indices[peer] -= 1 if @indices[peer] && @indices[peer] > 0
    end
  end
end
