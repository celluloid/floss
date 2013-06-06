require 'celluloid/proxies/abstract_proxy'
require 'raft/node'

# A {Raft::Proxy} wraps a FSM and runs it on a cluster.
class Raft::Proxy < Celluloid::AbstractProxy
  # @param [Object] fsm    The fsm to expose.
  # @param [Hash] options  Options as used by {Raft::Node}.
  def initialize(fsm, options)
    @fsm = fsm
    @node = ::Raft::Node.new(options) { |command| fsm.send(*command) }
  end

  # Executes all methods exposed by the FSM in the cluster.
  def method_missing(method, *args, &block)
    raise ArgumentError, "Can not accept blocks." if block_given?
    return super unless respond_to?(method)
    @node.wait_until_ready
    @node.execute([method, *args])
  end

  def respond_to?(method, include_private = false)
    @fsm.respond_to?(method, include_private)
  end
end

