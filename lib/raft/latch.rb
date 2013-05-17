require 'celluloid'
require 'raft'

# Based on Celluloid::Condition.
class Raft::Latch
  SignalConditionRequest = Celluloid::SignalConditionRequest
  class LatchError < Celluloid::Error; end

  def initialize
    @tasks = []
    @mutex = Mutex.new
    @ready = false
    @values = nil
  end

  def ready?
    @mutex.synchronize { @ready }
  end

  def wait
    raise LatchError, "cannot wait for signals while exclusive" if Celluloid.exclusive?

    task = Thread.current[:celluloid_actor] ? Celluloid::Task.current : Thread.current
    waiter = Celluloid::Condition::Waiter.new(self, task, Celluloid.mailbox)

    ready = @mutex.synchronize do
      ready = @ready
      @tasks << waiter unless ready
      ready
    end

    values = if ready
      @values
    else
      values = Celluloid.suspend(:condwait, waiter)
      raise values if values.is_a?(LatchError)
      values
    end

    values.size == 1 ? values.first : values
  end

  def signal(*values)
    @mutex.synchronize do
      return false if @ready

      @ready = true
      @values = values

      @tasks.each { |waiter| waiter << SignalConditionRequest.new(waiter.task, values) }
    end
  end
end
