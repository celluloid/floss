# -*- encoding: utf-8 -*-
require File.expand_path('../lib/floss/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Alexander Flatter"]
  gem.email         = ["flatter@fastmail.fm"]
  gem.description   = "Floss distributed consensus module for Celluloid"
  gem.summary       = "Floss is an implementation of the Raft distributed consensus protocol for Celluloid"
  gem.homepage      = "https://github.com/celluloid/floss"
  gem.license       = 'MIT'

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "floss"
  gem.require_paths = ["lib"]
  gem.version       = Floss::VERSION

  gem.add_development_dependency 'rspec'
  gem.add_development_dependency 'rake'
  gem.add_runtime_dependency 'celluloid-zmq'
  gem.add_runtime_dependency 'celluloid-io'
  gem.add_runtime_dependency 'kyotocabinet-ruby'
end
