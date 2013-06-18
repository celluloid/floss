require 'floss/rpc'

class Floss::RPC::InMemory
  class Client < Floss::RPC::Client
    include Celluloid

    attr_accessor :address

    def initialize(address)
      self.address = address
    end

    def call(command, payload)
      timeout(Floss::RPC::TIMEOUT) { actor.execute(command, payload) }
    rescue Celluloid::DeadActorError, Celluloid::Task::TimeoutError
      raise Floss::TimeoutError
    end

    def actor
      Celluloid::Actor[address]
    end
  end

  class Server < Floss::RPC::Server
    include Celluloid

    execute_block_on_receiver :initialize

    def initialize(address, &handler)
      super

      Actor[address] = Actor.current
    end

    def execute(command, payload)
      handler.call(command, payload)
    end
  end
end
