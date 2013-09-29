$: << File.expand_path('../../lib', __FILE__)

require 'logger'
require 'celluloid'
require 'fakeredis/rspec'

logfile = File.open(File.expand_path("../../log/test.log", __FILE__), 'w')
logfile.sync = true
Celluloid.logger = Logger.new(logfile)
Celluloid.shutdown_timeout = 1
