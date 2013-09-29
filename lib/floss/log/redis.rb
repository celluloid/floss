require 'forwardable'
require 'floss'
require 'floss/log'
require 'celluloid/redis'
require 'celluloid/io'

# See Section 5.3.
class Floss::Log
  class Redis < Floss::Log
    include Celluloid
    extend Forwardable

    DEFAULT_OPTIONS = {
      redis_port: 6379,
      redis_host: '127.0.0.1',
      redis_db: 0
    }

    def initialize(options={})
      @options = DEFAULT_OPTIONS.merge(options)
      @k = 'raft_log'
      @r = ::Redis.new(host: @options[:redis_host],
                       port: @options[:redis_port],
                       db: @options[:redis_db])
    end

    # @param [Array] The entries to append to the log.
    def append(new_entries)
      raise ArgumentError, 'The passed array is empty.' if new_entries.empty?
      new_entries.each do |e|
        @r.rpush(@k, Marshal.dump(e))
      end
      last_index
    end

    def empty?
      @r.llen(@k) == 0
    end

    def [](idx)
      v = @r.lindex(@k, idx)
      return nil if v.nil?
      Marshal.load(v)
    end

    def []=(idx,v)
      @r.lset(@k,idx,Marshal.dump(v))
    end

    def starting_with(index)
      @r.lrange(@k, index, -1).map do |e|
        Marshal.load(e)
      end
    end

    # Returns the last index in the log or nil if the log is empty.
    def last_index
      len = @r.llen(@k)
      return nil if len == 0
      len > 0 ? len - 1 : nil
    end

    # Returns the term of the last entry in the log or nil if the log is empty.
    def last_term
      return nil if empty?
      e = @r.lindex(@k,@r.llen(@k) - 1)
      entry = Marshal.load(e)
      entry ? entry.term : nil
    end

    def validate(index, term)
      # Special case: Accept the first entry if the log is empty.
      return empty? if index.nil? && term.nil?
      e = @r.lindex(@k, index)
      return nil if e.nil?
      entry = Marshal.load(e)
      entry && entry.term == term
    end

    def remove_starting_with(index)
      if index == 0
        @r.del(@k)
      else
        @r.ltrim(@k, index, -1)
      end
    end
  end
end
