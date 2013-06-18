require 'floss'
require 'floss/rpc/zmq'

# A peer is a remote node within the same cluster.
class Floss::Peer
  include Celluloid::Logger

  # @return [String] Remote address of the peer.
  attr_accessor :id

  # @return [Floss::RPC::Client]
  attr_accessor :client

  def initialize(id, opts = {})
    self.id = id

    client_class = opts[:rpc_client_class] || Floss::RPC::ZMQ::Client
    self.client = client_class.new(id)
  end

  def execute(payload)
    client.call(:execute, payload)
  end

  def append_entries(payload)
    client.call(:append_entries, payload)
  end

  def request_vote(payload)
    client.call(:vote_request, payload)
  end
end
