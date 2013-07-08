require 'floss/node'

describe Floss::Node do
  it "doesn't crash when all of its peers are down" do
    opts = {id: 'tcp://127.0.0.1:7001', peers: ['tcp://127.0.0.1:7002', 'tcp://127.0.0.1:7003']}
    node = described_class.new(opts)
    sleep(node.broadcast_time)
    expect(node).to be_alive
  end
end
