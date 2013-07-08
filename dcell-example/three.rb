#!/usr/bin/env ruby
require 'dcell'
require 'floss/dcell'

DCell.start(
  'id' => 'three',
  'addr' => 'tcp://127.0.0.1:9003',
  'registry' => {
    'adapter' => 'floss',
    'id' => 'tcp://127.0.0.1:9053',
    'peers' => ['tcp://127.0.0.1:9051', 'tcp://127.0.0.1:9052']
  }
)

class Three
  def value
    3
  end
end

Three.supervise_as(:one)
sleep
