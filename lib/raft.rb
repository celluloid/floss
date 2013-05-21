require 'celluloid'
require "raft/version"

module Raft
  class Error < StandardError; end
  class TimeoutError < Error; end
  class ServerUnavailableError < Error; end
end
