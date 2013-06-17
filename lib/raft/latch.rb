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
    @value = nil
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

    if ready
      @value
    else
      value = Celluloid.suspend(:condwait, waiter)
      raise value if value.is_a?(LatchError)
      value
    end
  end

  def signal(value)
    @mutex.synchronize do
      return false if @ready

      @ready = true
      @value = value

      @tasks.each { |waiter| waiter << SignalConditionRequest.new(waiter.task, value) }
    end
  end
end
