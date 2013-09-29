require 'floss/log'
require 'floss/log/simple'
require 'floss/log/redis'

shared_examples 'a Log implementation' do

  before do
    @log = described_class.new {}
    @log.remove_starting_with(0)
  end

  it 'returns empty when there are no entries' do
    @log.should be_empty
  end

  it 'appends entries' do
    entries = [Floss::Log::Entry.new('command1',1),
               Floss::Log::Entry.new('command2',1),
               Floss::Log::Entry.new('command3',1)
    ]
    @log.append(entries)
  end

  it 'can return an entry by index' do
    entries = [Floss::Log::Entry.new('command1',1)]
    @log.append(entries)
    entry = @log[0]
    entry.command.should eql('command1')
  end

  it 'can return a range of entries' do
    entries = [Floss::Log::Entry.new('command1',1),
               Floss::Log::Entry.new('command2',1),
               Floss::Log::Entry.new('command3',1)
    ]
    @log.append(entries)
    range = @log.starting_with(1)
    range.size.should eql(2)
    range[0].command.should eql('command2')
  end

  it 'can return index of the last entry' do
    @log.append([Floss::Log::Entry.new('command1',1),
                Floss::Log::Entry.new('command2',1)])
    idx = @log.last_index
    idx.should eql(1)
  end

  it 'returns the term of the last entry' do
    @log.append([Floss::Log::Entry.new('command1',1),
                Floss::Log::Entry.new('command2',1)])
    term = @log.last_term
    term.should eql(1)
  end

end

describe Floss::Log::Simple do
  it_should_behave_like 'a Log implementation'
end

describe Floss::Log::Redis do
  it_should_behave_like 'a Log implementation'
end
