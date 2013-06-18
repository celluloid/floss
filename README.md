# Floss

An implementation of the Raft concensus algorithm on top of Celluloid.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'raft'
```

And then execute:

```bash
$ bundle
```

Or install it yourself as:

```bash
$ gem install raft
```

## Usage

We're going to implement a distributed counter. While not very useful, it's a good enough demonstration of what you can
do with this library. Let's start with the counter service. It accepts three commands, `get`, `reset` and `increase`.
The first simply returns the current count, the second sets the count to zero and the third changes the current count,
optionally by a given amount.

```ruby
class Counter
  attr_accessor :count

  def initialize
    self.count = 0
  end

  def get
    count
  end

  def reset
    self.count = 0
  end

  def increase(amount = 1)
    self.count += amount
  end
end
```

To increase reliability of your counter, you decide to distribute it across multiple machines. This is where `raft`
comes into play. To simplify this demonstration, we're going to start multiple nodes in the same process.

```ruby
addresses = [10001, 10002, 10003].map { |port| "tcp://127.0.0.1:#{port}" }

$nodes = addresses.size.times.map do |i|
  combination = addresses.rotate(i)
  options = {listen: combination.first, peers: combination[1..-1]}
  Raft::Proxy.new(Counter.new, options)
end

# Give your nodes some time to start up.
$nodes.each(&:wait_until_ready)
```

Now we're ready to play with our distributed counter.

```ruby
def random_node; $nodes.sample; end

random_node.get # => 0
random_node.increase # => 1
random_node.get # => 1
```

That was easy wasn't it? Let's see what happens if the cluster is damaged.

```ruby
# Terminate a random node in the cluster.
doomed_node = $nodes.delete(random_node)
doomed_node_id = doomed_node.id
doomed_node.terminate

random_node.increase # => 2
```

Your cluster still works. If you'd kill another one, executing a command would result in an error because insufficient
nodes are available to ensure your system's consistency.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
