require 'floss'

module Floss::TestHelper
  extend self

  # Takes a list of node ids and yields a list of peers for each id.
  def cluster(ids, &block)
    cluster_size = ids.size

    cluster_size.times.map do |i|
      combination = ids.rotate(i)
      block.call(combination.first, combination[1..-1])
    end
  end
end
