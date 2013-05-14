require 'raft/rpc'

# Listens to a ZMQ Socket and handles commands from peers.
class Raft::RPC::Server
  include Celluloid::ZMQ
  include Celluloid::Logger

  attr_accessor :address
  attr_accessor :handler
  attr_accessor :socket

  def initialize(address, &handler)
    self.address = address
    self.handler = handler
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
    loop { async.handle(socket.read) }
  end

  # @param [String] request  A request string containing command and payload separated by a colon.
  def handle(request)
    command, payload = request.split(':', 2)

    response = case command
               when 'PING'
                 'PONG'
               else
                 payload = Marshal.load(payload)
                 handler.call(command, payload)
               end

    socket.write(Marshal.dump(response))
  end
end

#class Raft::Node::Server < Reel::Server
#  def initialize(address)
#    super(host, port, &method(:on_connection))
#  end
#
#
#  def on_connection(connection)
#    while request = connection.request
#      case request
#      when Reel::Request
#        handle_request(request)
#      when Reel::WebSocket
#        handle_websocket(request)
#      end
#    end
#  end
#
#  def handle_request(request)
#    info request.path
#    info request.url
#    request.respond :ok, "Hello, world!"
#  end
#end
