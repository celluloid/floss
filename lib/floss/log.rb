require 'forwardable'
require 'floss'

# See Section 5.3.
class Floss::Log
  include Celluloid
  extend Forwardable

  def_delegators :entries, :[], :empty?

  class Entry
    # @return [Fixnum] When the entry was received by the leader.
    attr_accessor :term

    # @return [Object] A replicated state machine command.
    attr_accessor :command

    def initialize(command, term)
      raise ArgumentError, "Term must be a Fixnum." unless term.is_a?(Fixnum)

      self.term = term
      self.command = command
    end
  end

  # @return [Array<Entry>] The log's entries.
  attr_accessor :entries

  def initialize
    self.entries = []
  end

  # @param [Array] The entries to append to the log.
  def append(new_entries)
    raise ArgumentError, 'The passed array is empty.' if new_entries.empty?

    entries.concat(new_entries)
    last_index
  end

  def starting_with(index)
    entries[index..-1]
  end

  # Returns the last index in the log or nil if the log is empty.
  def last_index
    entries.any? ? entries.size - 1 : nil
  end

  # Returns the term of the last entry in the log or nil if the log is empty.
  def last_term
    entry = entries.last
    entry ? entry.term : nil
  end

  def complete?(other_term, other_index)
    # Special case: Accept the first entry if the log is empty.
    return empty? if other_term.nil? && other_index.nil?

    (other_term > last_term) || (other_term == last_term && other_index >= last_index)
  end

  def validate(index, term)
    # Special case: Accept the first entry if the log is empty.
    return empty? if index.nil? && term.nil?

    entry = entries[index]
    entry && entry.term == term
  end

  def remove_starting_with(index)
    entries.slice!(index..-1)
  end
end
