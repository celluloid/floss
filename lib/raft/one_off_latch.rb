require 'raft'

class Raft::OneOffLatch
  attr_accessor :ready
  attr_accessor :condition

  def initialize
    self.ready = false
    self.condition = Celluloid::Condition.new
  end

  def signal
    return if ready

    self.ready = true
    condition.broadcast
  end

  def wait
    return if ready
    condition.wait
  end
end
