require 'raft'

# A peer is a remote node within the same cluster.
class Raft::Peer
  include Celluloid::Logger

  # @return [String] Remote address of the peer.
  attr_accessor :id

  # @return [Celluloid::ZMQ::ReqSocket]
  # attr_accessor :socket

  # @return [Raft::RPC::Client]
  attr_accessor :client

  def initialize(id, opts = {})
    self.id = id

    client_class = opts[:client_class] || Raft::RPC::ZMQ::Client
    self.client = client_class.new(id)
  end

  def append_entries(payload)
    client.call(:append_entries, payload)
  end

  def request_vote(payload)
    client.call(:vote_request, payload)
  end
end
