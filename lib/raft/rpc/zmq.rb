require 'raft/rpc'
require 'celluloid/io/stream'
require 'celluloid/zmq'

class Raft::RPC::ZMQ
  class Client < Raft::RPC::Client
    # @return [Celluloid::IO::Stream::Latch]
    attr_accessor :latch

    # @return [Celluloid::ZMQ::ReqSocket]
    attr_accessor :socket

    def initialize(address)
      self.latch = Celluloid::IO::Stream::Latch.new
      self.socket = Celluloid::ZMQ::ReqSocket.new
      socket.connect(address)
    end

    def call(command, payload)
      request = encode_request(command, payload)
      response = latch.synchronize do
        socket.send(request)
        socket.read
      end
      decode_response(response)
    end

    def encode_request(command, payload)
      "#{command}:#{Marshal.dump(payload)}"
    end

    def decode_response(response)
      Marshal.load(response)
    end

    def disconnect
      socket.close if socket
    end
  end

  class Server < Raft::RPC::Server
    include Celluloid::ZMQ
    include Celluloid::Logger

    attr_accessor :socket

    execute_block_on_receiver :initialize
    finalizer :finalize

    def initialize(address, &handler)
      super
      async.run
    end

    def run
      self.socket = RepSocket.new

      begin
        info("Binding to #{address}")
        socket.bind(address)
      rescue IOError
        socket.close
        raise
      end

      async.loop!
    end

    def loop!
      loop { handle(socket.read) }
    end

    # @param [String] request  A request string containing command and payload separated by a colon.
    def handle(request)
      p request
      command, payload = decode_request(request)
      response = handler.call(command, payload)
      socket.send(encode_response(response))
    end

    def encode_response(response)
      Marshal.dump(response)
    end

    def decode_request(request)
      command, payload = request.split(':', 2)
      payload = Marshal.load(payload)

      [command.to_sym, payload]
    end

    def finalize
      socket.close if socket
    end

    def terminate
      super
      socket.close if socket
    end
  end
end
