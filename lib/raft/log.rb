require 'forwardable'
require 'raft'

# See Section 5.3.
class Raft::Log
  extend Forwardable

  def_delegators :entries, :[]

  class Entry
    # @return [Fixnum] When the entry was received by the leader.
    attr_accessor :term

    # @return [Object] A replicated state machine command.
    attr_accessor :command

    # @return [Boolean]
    attr_accessor :committed

    def committed?

    end
  end

  # @return [Array<Entry>] The log's entries.
  attr_accessor :entries

  def initialize
    self.entries = []
  end

  def append(prev_log_index, prev_log_term, new_entries)
    if validate(prev_log_index, prev_log_term)
      # Remove all entries after the previous one.
      remove_starting_with(prev_log_index + 1)
      entries.concat(new_entries)

      true
    else
      false
    end
  end

  def last_index
    entries.size - 1
  end

  def last_term
    entry = entries.last
    entry ? entry.term : -1
  end

  def complete?(other_term, other_index)
    (other_term > last_term) || (other_term == last_term && other_index >= last_index)
  end

  protected

  def validate(index, term)
    entry = entries[index]
    entry && entry.term == term
  end

  def remove_starting_with(index)
    entries.slice!(index..-1)
  end
end
