require 'floss'

module Floss::RPC
  TIMEOUT = 0.3

  class Client
    def call(command, payload)
      raise NotImplementedError
    end
  end

  # Listens to a ZMQ Socket and handles commands from peers.
  class Server
    attr_accessor :address
    attr_accessor :handler

    def initialize(address, &handler)
      self.address = address
      self.handler = handler
    end
  end
end
