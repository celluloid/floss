require 'celluloid'
require "floss/version"

module Floss
  class Error < StandardError; end
  class TimeoutError < Error; end
end
