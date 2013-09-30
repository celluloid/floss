require 'floss'
require 'floss/count_down_latch'

# Used by the leader to manage the replicated log.
class Floss::LogReplicator
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
      # This class is always used to wait for replication of a log entry, so we're failing fast here:
      # Every log entry has an index, thus nil is disallowed.
      unless index.is_a?(Fixnum) && index >= 0
        raise ArgumentError, 'index must be a Fixnum and >= 0'
      end

      @waiters << Waiter.new(peer, index, condition)
    end

    def signal(peer, index)
      return unless index # There's nothing to do if no index is given, see #register.

      waiters = @waiters.delete_if do |waiter|
        next unless waiter.peer == peer
        waiter.index <= index
      end

      waiters.map(&:condition).each(&:signal)
    end
  end

  finalizer :finalize

  def_delegators :node, :peers, :log

  # TODO: Pass those to the constructor: They don't change during a term.
  def_delegators :node, :cluster_quorum, :broadcast_time, :current_term

  # @return [Floss::Node]
  attr_accessor :node

  def initialize(node)
    @node = node

    # A helper for waiting on a certain index to be written to a peer.
    @write_waiters = IndexWaiter.new

    # Stores the index of the last log entry that a peer agrees with.
    @write_indices = {}

    # Keeps Celluloid::Timer instances that fire periodically for each peer to trigger replication.
    @pacemakers = {}

    initial_write_index = log.last_index

    peers.each do |peer|
      @write_indices[peer] = initial_write_index
      @pacemakers[peer] = after(broadcast_time) { replicate(peer) }
    end
  end

  def append(entry)
    pause
    index = log.append([entry])

    quorum = Floss::CountDownLatch.new(cluster_quorum)
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
    write_index = @write_indices[peer]
    response = peer.append_entries(construct_payload(write_index))

    if response[:success]
      # nil if the log is still empty, last replicated log index otherwise
      last_index = log.last_index

      @write_indices[peer] = last_index
      @write_waiters.signal(peer, last_index)
    else
      # Walk back in the peer's history.
      @write_indices[peer] = write_index > 0 ? write_index - 1 : nil if write_index
    end

    @pacemakers[peer].reset
  end

  # Constructs payload for an AppendEntries RPC given a peer's write index.
  # All entries **after** the given index will be included in the payload.
  def construct_payload(index)
    if index
      prev_index = index
      prev_term  = log[prev_index].term
      entries = log.starting_with(index + 1)
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
