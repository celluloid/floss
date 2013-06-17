$: << File.expand_path('../../lib', __FILE__)

require 'raft/test_helper'
require 'raft/proxy'

include Celluloid::Logger

class FSM
  def initialize
    @content = Hash.new
  end

  def set(key, value)
    @content[key] = value
  end

  def get(key)
    @content[key]
  end
end

CLUSTER_SIZE = 5

ids = CLUSTER_SIZE.times.map do |i|
  port = 50000 + i
  "tcp://127.0.0.1:#{port}"
end

proxies = Raft::TestHelper.cluster(ids) do |id, peers|
  Raft::Proxy.new(FSM.new, id: id, peers: peers)
end

100.times do |i|
  proxies.sample.set(:foo, i)
  raise "fail" unless proxies.sample.get(:foo) == i
end
