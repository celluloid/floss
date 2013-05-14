$: << File.expand_path('../lib', __FILE__)

require 'raft/node'

cluster = ['tcp://127.0.0.1:50010', 'tcp://127.0.0.1:50011', 'tcp://127.0.0.1:50012']

cluster.size.times do |i|
  nodes = cluster.rotate(i)

  Raft::Node.new(listen: nodes.first, peers: nodes[1..-1]).run
end

sleep
