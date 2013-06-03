# encoding: utf-8

require 'raft/rpc/zmq'
require 'raft/log'
require 'raft/one_off_latch'

class Raft::Node
  require 'raft/node/states'

  include Celluloid
  include Celluloid::Logger

  # Default broadcast time.
  # @see #broadcast_time
  BROADCAST_TIME = 0.020

  # Default election timeout.
  # @see #election_timeout
  ELECTION_TIMEOUT = (0.150..0.300)

  # @return [Raft::Log] The replicated log.
  attr_accessor :log

  # @return [Hash]
  attr_accessor :options

  # @return [Raft::Node::State::Base] The current state of this node.
  attr_accessor :state

  # @return [String] id of this node's candidate
  attr_accessor :voted_for

  # @return [Fixnum] The current term.
  attr_accessor :term

  # @return [Raft::RPC::Server]
  attr_accessor :server

  # @return [Raft::RPC::Client]
  attr_accessor :client

  # @return [Raft::OneOffLatch]
  attr_accessor :ready

  DEFAULT_OPTIONS = {
    rpc: Raft::RPC::ZMQ,
    run: true
  }.freeze

  # @param [Hash] options
  # @option options [String] :id            A string identifying this node, often its RPC address.
  # @option options [Array<String>] :peers  Identifiers of all peers in the cluster.
  # @option options [Module,Class] :rpc     Namespace containing `Server` and `Client` classes.
  def initialize(options = {}, &handler)
    self.options = options = DEFAULT_OPTIONS.merge(options)

    self.term = 0
    self.log = Raft::Log.new
    self.ready = Raft::OneOffLatch.new

    async.run if options[:run]
  end

  def run
    self.server = link(rpc_server_class.new(id, &method(:handle_rpc)))
    switch_state(:follower)
  end

  # Blocks until the node is ready for executing commands.
  def wait_until_ready
    ready.wait
  end

  def rpc_server_class
    options[:rpc].const_get('Server')
  end

  def rpc_client_class
    options[:rpc].const_get('Client')
  end

  # Execute a command on the replicated state machine.
  def execute(*args)
    state.execute(*args)
  end

  def handle_rpc(command, payload)
    state.handle_rpc(command, payload)
  end

  def switch_state(new_state)
    current_state = state ? state.class.name.split('::').last.downcase : 'nil'
    info("[TRANSITION] #{current_state} => #{new_state}")

    self.state.exit_state if state
    self.state = State.const_get(new_state.to_s.capitalize).new(Actor.current)
    self.state.enter_state
  end

  # Returns this node's id.
  # @return [String]
  def id
    options[:id]
  end

  # Returns peers in the cluster.
  # @return [Array<Raft::Peer>]
  def peers
    @peers ||= options[:peers].map { |peer| Raft::Peer.new(peer, rpc_client_class: rpc_client_class) }
  end

  def terminate
    state.exit_state if state
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
    options[:broadcast_time] || BROADCAST_TIME
  end

  # Election timeout as defined in Section 5.2.
  #
  # This timeout is used in multiple ways:
  #
  #   * If a follower does not receive any activity, it starts a new election.
  #   * As a candidate, if the election does not resolve within this time, it is restarted.
  #
  # @return [Range<Float>]
  def election_timeout
    options[:election_timeout] || ELECTION_TIMEOUT
  end

  # A random timeout within {#election_timeout}.
  # @return [Float]
  def random_timeout
    min, max = election_timeout.first, election_timeout.last

    min + rand(max - min)
  end

  def validate_term(other_term)
    if other_term < term
      return false
    end

    if other_term > term
      enter_new_term(other_term)
    end

    true
  end

  def enter_new_term(new_term = nil)
    self.term = (new_term || term + 1)
    self.voted_for = nil
  end

  def respond_to?(symbol, include_all = false)
    super || state.respond_to?(symbol, include_all)
  end

  def method_missing(symbol, *arguments, &block)
    if state.respond_to?(symbol)
      return state.send(symbol, *arguments, &block)
    end

    super
  end

  %w(info debug warn error).each do |m|
    define_method(m) do |str|
      super("[#{id}] #{str}")
    end
  end
end
