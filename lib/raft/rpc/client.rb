require 'celluloid/zmq'
require 'raft/rpc'

class Raft::RPC::Client
  include Celluloid::ZMQ
  include Celluloid::Logger

  attr_accessor :peer
  attr_accessor :timer

  def call(peer, command, payload)
    socket = ReqSocket.new
    socket.connect(peer)
    socket.send(encode_request(command, payload))
    decode_response(socket.read)
  ensure
    socket.close if socket
  end

  # Send a vote request RPC to a peer and retry until it responds.
  def vote_request(peer, payload)
    # FIXME: Retry
    # try_every(0.05) do
      call(peer, :vote_request, payload)
    # end
  end

  def append_entries(peer, payload)
    call(peer, :append_entries, payload)
  end

  def try_every(timeout, &block)
    condition = Celluloid::Condition.new

    timer = every(timeout) do
      if result = block.call
        timer.cancel
        condition.signal(result)
      end
    end

    condition.wait
  end

  # Sends a message to a list of peers and returns responses.
  # Partial results will be returned if some peers do not respond in time.
  def multicall(peers, method, payload, timeout)
    condition = Condition.new
    results = {}

    peers.map do |peer|
      future(method, peer, payload).tap do |future|
        async.signal_when_done(condition, future, peer)
      end
    end

    after(timeout) { condition.signal(:timeout) }

    loop do
      result, response, peer = condition.wait

      # Return partial results on timeout.
      break if result == :timeout

      results[peer] = response

      # Return results if all peers responded.
      break if results.size == peers.size
    end

    results
  end

  def signal_when_done(condition, future, key)
    condition.signal([key, future.value])
  end

#  def try_every(timeout, &block)
#    condition = Celluloid::Condition.new
#    timer = after(0.1) { condition.signal(Celluloid::TimeoutError) }
#
#    begin
#      Celluloid::Future.new do
#        condition.signal(block.call)
#      end
#
#      condition.wait
#    ensure
#      timer.cancel if timer
#    end
#  end
#
  def encode_request(command, payload)
    "#{command}:#{Marshal.dump(payload)}"
  end

  def decode_response(response)
    Marshal.load(response)
  end
end

