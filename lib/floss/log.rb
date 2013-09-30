require 'forwardable'
require 'floss'

# See Section 5.3.
class Floss::Log
  include Celluloid
  extend Forwardable

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

  def initialize(options={})
    raise NotImplementedError
  end

  def []=(k,v)
    raise NotImplementedError
  end

  def empty?
    raise NotImplementedError
  end

  # @param [Array] The entries to append to the log.
  def append(new_entries)
    raise NotImplementedError
  end

  def starting_with(index)
    raise NotImplementedError
  end

  # Returns the last index in the log or nil if the log is empty.
  def last_index
    raise NotImplementedError
  end

  # Returns the term of the last entry in the log or nil if the log is empty.
  def last_term
    raise NotImplementedError
  end

  def complete?(other_term, other_index)
    # Special case: Accept the first entry if the log is empty.
    return empty? if other_term.nil? && other_index.nil?

    (other_term > last_term) || (other_term == last_term && other_index >= last_index)
  end

  def validate(index, term)
    raise NotImplementedError
  end

  def remove_starting_with(index)
    raise NotImplementedError
  end
end
