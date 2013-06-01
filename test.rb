$: << File.expand_path('../lib', __FILE__)

require 'raft/node'

CLUSTER_SIZE = 5

nodes = CLUSTER_SIZE.times.map do |i|
  port = 50000 + i
  "tcp://127.0.0.1:#{port}"
end

supervisor = Celluloid::SupervisionGroup.run!

CLUSTER_SIZE.times.map do |i|
  combination = nodes.rotate(i)
  options = {id: combination.first, peers: combination[1..-1]}
  supervisor.supervise(Raft::Node, options)
end

sleep
