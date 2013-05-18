require 'raft'
require 'celluloid/zmq'

# A peer is a remote node within the same cluster.
class Raft::Peer
  include Celluloid::ZMQ
  include Celluloid::Logger

  exclusive :call

  # @return [String] Remote address of the peer.
  attr_accessor :id

  # @return [Celluloid::ZMQ::ReqSocket]
  attr_accessor :socket

  def initialize(id)
    self.id = id
    connect
  end

  def connect
    socket = ReqSocket.new
    socket.connect(id)
    self.socket = socket
  end

  def disconnect
    socket.close if socket
    self.socket = nil
  end

  def append_entries(payload)
    call(:append_entries, payload)
  end

  def request_vote(payload)
    call(:vote_request, payload)
  end

  def call(command, payload)
    socket.send(encode_request(command, payload))
    decode_response(socket.read)
  end

  def encode_request(command, payload)
    "#{command}:#{Marshal.dump(payload)}"
  end

  def decode_response(response)
    Marshal.load(response)
  end
end
