require 'gooddata_datawarehouse/datawarehouse'
require_relative 'spec_helper'

describe GoodData::Datawarehouse do
  before(:each) do
    @dwh = SpecHelper::create_default_connection
    @random = rand(10000000).to_s
  end

  describe '#create_table' do
    it 'creates a table' do
      table_name = "table_#{@random}"
      @dwh.create_table(table_name, ['col1', 'col2', 'col3'])
require 'pry'; binding.pry
    end
  end
end