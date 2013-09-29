$: << File.expand_path('../../lib', __FILE__)

require 'floss/test_helper'
require 'floss/proxy'
require 'floss/log/simple'
require 'floss/log/redis'

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

proxies = Floss::TestHelper.cluster(ids) do |id, peers|
  db = id.split(':').last.to_i - 50000
  Floss::Proxy.new(FSM.new, id: id, peers: peers, log: Floss::Log::Redis, redis_db: db)
end

100.times do |i|
  proxies.sample.set(:foo, i)
  raise "fail" unless proxies.sample.get(:foo) == i
end
