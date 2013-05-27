# Raft

An implementation of the Raft concensus algorithm on top of Celluloid.

## Installation

Add this line to your application's Gemfile:

    gem 'raft'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install raft

## Usage

We're going to implement a distributed counter. While not very useful, it's a good enough demonstration of what you can
do with this library. Let's start with the counter service. It accepts three commands, `get`, `reset` and `increase`.
The first simply returns the current count, the second sets the count to zero and the third changes the current count,
optionally by a given amount.

    class Counter
      attr_accessor :count

      def initialize
        self.count = 0
      end

      # Handle commands accepted by your cluster.
      def execute(payload)
        case command = payload[:command]
        when 'GET'
          self.count
        when 'RESET'
          self.count = 0
        when 'INCREASE'
          self.count += payload[:amount] || 1
        else
          "Invalid command: #{command}."
        end
      end
    end

To increase reliability of your counter, you decide to distribute it across multiple machines. This is where `raft`
comes into play.

    addresses = [10001, 10002, 10003].map { |port| "tcp://127.0.0.1:#{port}" }

    nodes = addresses.size.times.map do |i|
      combination = addresses.rotate(i)
      options = {listen: combination.first, peers: combination[1..-1]}
      Raft::Node.new(listen: combination.first, peers: combination[1..-1]).tap { |node| node.run }
    end

    # Give your nodes some time to start up.
    sleep 0.5

    cluster.execute(command: 'GET') # => 0
    cluster.execute(command: 'INCREASE') # => 1
    cluster.execute(command: 'GET') # => 1

That was easy wasn't it? Let's see what happens if the cluster is damaged.

    # Terminate a random node in the cluster.
    doomed_node = nodes.delete(nodes.sample)
    doomed_node_id = doomed_node.id
    doomed_node.terminate

    cluster.execute(command: 'INCREASE') # => 2

Your cluster still works. If you'd kill another one, executing a command would result in an error because not enough
nodes are available to ensure your system's consistency.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
