require 'forwardable'
require 'floss'
require 'floss/log'
require 'celluloid/io'
require 'kyotocabinet'

# See Section 5.3.
class Floss::Log
  class Kyoto < Floss::Log
    include Celluloid
    include Celluloid::Logger
    extend Forwardable

    DEFAULT_OPTIONS = {
      kyoto_db: '/tmp/floss.kch'
    }

    finalizer :finalize

    def initialize(options={})
      @options = DEFAULT_OPTIONS.merge(options)
      @db = KyotoCabinet::DB.new
      @db.open(@options[:kyoto_db])
      info "Openned log at #{@options[:kyoto_db]}"
    end

    # @param [Array] The entries to append to the log.
    def append(new_entries)
      raise ArgumentError, 'The passed array is empty.' if new_entries.empty?
      cur_idx = @db.count
      new_entries.each_with_index do |e, idx|
        @db[(cur_idx + idx).to_s] = Marshal.dump(e)
      end
      last_index
    end

    def empty?
      @db.count == 0
    end

    def [](index)
      v = @db[index.to_s]
      return nil if v.nil?
      Marshal.load(v)
    end

    def []=(index,v)
      @db[index.to_s] = Marshal.dump(v)
    end

    def starting_with(index)
      cur = @db.cursor
      cur.jump(index.to_s)
      records = []
      while rec = cur.get(true)
        records << Marshal.load(rec[1])
      end
      records
    end

    # Returns the last index in the log or nil if the log is empty.
    def last_index
      len = @db.count
      return nil if len == 0
      len > 0 ? len - 1 : nil
    end

    # Returns the term of the last entry in the log or nil if the log is empty.
    def last_term
      return nil if empty?
      e = @db[last_index.to_s]
      return nil if e.nil?
      entry = Marshal.load(e)
      entry ? entry.term : nil
    end

    def validate(index, term)
      # Special case: Accept the first entry if the log is empty.
      return empty? if index.nil? && term.nil?
      e = @db[index]
      return false if e.nil?
      entry = Marshal.load(e)
      entry && entry.term == term
    end

    def remove_starting_with(index)
      (index..@db.count).each do |idx|
        @db.remove(idx.to_s)
      end
    end

    def finalize
      @db.close
    end
  end
end
