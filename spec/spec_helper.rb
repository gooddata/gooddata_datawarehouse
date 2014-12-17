require 'gooddata_datawarehouse'

RSpec.configure do |c|
  c.filter_run :focus => true
  c.run_all_when_everything_filtered = true
end

class SpecHelper
  def self.create_default_connection
    GoodData::Datawarehouse.new(ENV['USERNAME'], ENV['PASSWORD'], ENV['INSTANCE_ID'])
  end
end