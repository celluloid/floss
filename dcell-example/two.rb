#!/usr/bin/env ruby
require 'dcell'
require 'floss/dcell'

DCell.start(
  'id' => 'two',
  'addr' => 'tcp://127.0.0.1:9002',
  'registry' => {
    'adapter' => 'floss',
    'id' => 'tcp://127.0.0.1:9052',
    'peers' => ['tcp://127.0.0.1:9051', 'tcp://127.0.0.1:9053']
  }
)

class Two
  def value
    2
  end
end

Two.supervise_as(:two)
sleep
