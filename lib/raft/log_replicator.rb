require 'raft'
require 'raft/count_down_latch'

# Used by the leader to manage the replicated log.
class Raft::LogReplicator
  extend Forwardable
  include Celluloid

  class IndexWaiter
    class Waiter
      attr_reader :peer
      attr_reader :index
      attr_reader :condition

      def initialize(peer, index, condition)
        @peer = peer
        @index = index
        @condition = condition
      end
    end

    def initialize
      @waiters = []
    end

    def register(peer, index, condition)
      @waiters << Waiter.new(peer, index, condition)
    end

    def signal(peer, index)
      waiters = @waiters.delete_if { |waiter| waiter.peer == peer && (index && waiter.index < index) }
      waiters.map(&:condition).each(&:signal)
    end
  end

  finalizer :finalize

  def_delegators :node, :peers, :log
  
  # TODO: Pass those to the constructor: They don't change during a term.
  def_delegators :node, :cluster_quorum, :broadcast_time, :current_term

  # @return [Raft::Node]
  attr_accessor :node

  def initialize(node)
    self.node = node

    @write_waiters = IndexWaiter.new
    @write_indices = {}
    @pacemakers = {}

    last_index = log.last_index
    initial_write_index = last_index ? last_index + 1 : nil

    peers.each do |peer|
      @write_indices[peer] = initial_write_index
      @pacemakers[peer] = after(broadcast_time) { replicate(peer) }
    end
  end

  def append(entry)
    pause
    index = log.append([entry])

    quorum = Raft::CountDownLatch.new(cluster_quorum)
    peers.each { |peer| signal_on_write(peer, index, quorum) }

    resume
    quorum.wait

    # TODO: Ensure there's at least one write in the leader's current term.
    @commit_index = index
  end

  def signal_on_write(peer, index, condition)
    @write_waiters.register(peer, index, condition)
  end

  def pause
    @pacemakers.values.each(&:cancel)
  end

  def resume
    @pacemakers.values.each(&:fire)
  end

  def replicate(peer)
    index = @write_indices[peer]

    last_index = log.last_index
    response = peer.append_entries(construct_payload(index))

    if response[:success]
      @write_indices[peer] = last_index ? last_index + 1 : nil
      @write_waiters.signal(peer, last_index)
    else
      @write_indices[peer] -= 1
    end

    @pacemakers[peer].reset
  end

  def construct_payload(index)
    if index
      prev_index = index - 1
      prev_term  = log[prev_index].term
      entries = log.starting_with(index)
    else
      prev_index = nil
      prev_term = nil
      entries = log.starting_with(0)
    end

    Hash[
      leader_id: node.id,
      term: current_term,
      prev_log_index: prev_index,
      prev_log_term: prev_term,
      commit_index: @commit_index,
      entries: entries
    ]
  end

  def finalize
    pause
  end
end
