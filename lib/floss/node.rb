# encoding: utf-8

require 'floss/rpc/zmq'
require 'floss/log/simple'
require 'floss/log'
require 'floss/peer'
require 'floss/one_off_latch'
require 'floss/count_down_latch'
require 'floss/log_replicator'

class Floss::Node
  include Celluloid
  include Celluloid::FSM
  include Celluloid::Logger

  execute_block_on_receiver :initialize
  finalizer :finalize

  state(:follower, default: true, to: :candidate)

  state(:candidate, to: [:leader, :follower]) do
    enter_new_term
    start_election
  end

  state(:leader, to: [:follower]) do
    start_log_replication
  end

  # Default broadcast time.
  # @see #broadcast_time
  BROADCAST_TIME = 0.020

  # Default election timeout.
  # @see #election_timeout
  ELECTION_TIMEOUT = (0.150..0.300)

  # @return [Floss::Log] The replicated log.
  attr_reader :log

  attr_reader :current_term

  # @return [Floss::RPC::Server]
  attr_accessor :server

  DEFAULT_OPTIONS = {
    rpc: Floss::RPC::ZMQ,
    log: Floss::Log::Simple,
    run: true
  }.freeze

  # @param [Hash] options
  # @option options [String] :id            A string identifying this node, often its RPC address.
  # @option options [Array<String>] :peers  Identifiers of all peers in the cluster.
  # @option options [Module,Class] :rpc     Namespace containing `Server` and `Client` classes.
  def initialize(options = {}, &handler)
    super

    @handler = handler
    @options = DEFAULT_OPTIONS.merge(options)
    @current_term = 0
    @ready_latch = Floss::OneOffLatch.new
    @running = false

    async.run if @options[:run]
  end

  def run
    raise 'Already running' if @running

    @running = true
    @log = @options[:log].new @options

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
  # @return [Array<Floss::Peer>]
  def peers
    @peers ||= @options[:peers].map { |peer| Floss::Peer.new(peer, rpc_client_class: rpc_client_class) }
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
      self.state == name
    end
  end

  def execute(entry)
    if leader?
      entry = Floss::Log::Entry.new(entry, @current_term)
      @log_replicator.append(entry)
      @handler.call(entry.command) if @handler
    else
      raise "Cannot redirect command because leader is unknown." unless @leader_id
      leader = peers.find { |peer| peer.id == @leader_id }
      leader.execute(entry)
    end
  end

  def wait_for_quorum_commit(index)
    latch = Floss::CountDownLatch.new(cluster_quorum)
    peers.each { |peer| peer.signal_on_commit(index, latch) }
    latch.wait
  end

  def handle_rpc(command, payload)
    handler = :"handle_#{command}"

    if respond_to?(handler, true)
      send(handler, payload)
    else
      abort ArgumentError.new('Unknown command.')
    end
  end

  protected

  def handle_execute(entry)
    raise 'Only the leader can accept commands.' unless leader?
    execute(entry)
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

    # Remember the leader.
    @leader_id = payload[:leader_id]

    # A valid AppendEntries RPC resets the election timeout.
    @election_timeout.reset

    success = if payload[:entries].any?
      if log.validate(payload[:prev_log_index], payload[:prev_log_term])
        log.append(payload[:entries])
        true
      else
        false
      end
    else
      true
    end

    if payload[:commit_index] && @handler
      (@commit_index ? @commit_index + 1 : 0).upto(payload[:commit_index]) do |index|
        @handler.call(log[index].command) if @handler
      end
    end

    @commit_index = payload[:commit_index]

    unless success
      debug("[RPC] I did not accept AppendEntries: #{payload}")
    end

    return {term: @current_term, success: success}
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
    @votes = Floss::CountDownLatch.new(cluster_quorum)
    collect_votes

    @votes.wait

    transition(:leader)

    # Marks the node as ready for accepting commands.
    @ready_latch.signal
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
    response = begin
                 peer.request_vote(payload)
               rescue Floss::TimeoutError
                 debug("A vote request to #{peer.id} timed out. Retrying.")
                 retry
               end

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
    raise "A log replicator is already running." if @log_replicator
    @log_replicator = link Floss::LogReplicator.new(current_actor)
  end

  def stop_log_replication
    @log_replicator.terminate
    @log_replicator = nil
  end

  def finalize
    @log_replicator.terminate if @log_replicator
  end
end
