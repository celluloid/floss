#!/usr/bin/env ruby
require 'dcell'
require 'floss/dcell'

DCell.start(
  'id' => 'one',
  'addr' => 'tcp://127.0.0.1:9001',
  'registry' => {
    'adapter' => 'floss',
    'id' => 'tcp://127.0.0.1:9051',
    'peers' => ['tcp://127.0.0.1:9052', 'tcp://127.0.0.1:9053']
  }
)

class One
  def value
    1
  end
end

One.supervise_as(:one)

loop do
  puts DCell::Node[:one][:one].value + DCell::Node[:two][:two].value
end
