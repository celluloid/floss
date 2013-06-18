require 'celluloid'
require 'floss'

class Floss::CountDownLatch
  # @return [Fixnum] Current count.
  attr_reader :count

  def initialize(count)
    @count = count
    @condition = Celluloid::Condition.new
  end

  def signal
    return if @count == 0 

    @count -= 1
    @condition.signal if @count == 0
  end

  def wait
    @condition.wait
  end
end
