# -*- encoding: utf-8 -*-
require File.expand_path('../lib/raft/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Alexander Flatter"]
  gem.email         = ["flatter@fastmail.fm"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "raft"
  gem.require_paths = ["lib"]
  gem.version       = Raft::VERSION

  gem.add_development_dependency 'rspec'
  gem.add_runtime_dependency 'celluloid-zmq'
end
