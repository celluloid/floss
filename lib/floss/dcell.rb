require 'floss/proxy'
require 'dcell'

class DCell::Registry::FlossAdapter
  class FSM
    NAMESPACES = [:node, :global]

    def initialize
      @content = NAMESPACES.each_with_object(Hash.new) { |namespace, hash| hash[namespace] = Hash.new }
    end

    def set(namespace, key, value)
      @content[key] = value
    end

    def get(namespace, key)
      @content[key]
    end

    def keys(namespace)
      @content[namespace].keys
    end
  end

  def initialize(options)
    # Convert all options to symbols :/
    options = options.inject({}) { |h,(k,v)| h[k.to_sym] = v; h }

    @cluster = Floss::Proxy.new(FSM.new, id: options[:id], peers: options[:peers])
  end

  def get_node(node_id)
    @cluster.get(:node, node_id)
  end

  def set_node(node_id, addr)
    @cluster.set(:node, node_id, addr)
  end

  def nodes
    @cluster.keys(:node)
  end

  def get_global(key)
    @cluster.get(:global, key)
  end

  def set_global(key, value)
    @cluster.set(:global, key, value)
  end

  def global_keys
    @cluster.keys(:global)
  end
end
