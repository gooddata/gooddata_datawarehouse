require 'tempfile'
require 'gooddata_datawarehouse/datawarehouse'
require_relative 'spec_helper'

describe GoodData::Datawarehouse do
  before(:each) do
    @dwh = SpecHelper::create_default_connection
    @random = rand(10000000).to_s
    @random_table_name = "temp_#{@random}"
    @created_tables = nil
  end

  after(:each) do
    @created_tables.each{|t| @dwh.drop_table(t)} if @created_tables
  end

  describe '#create_table' do
    it 'creates a table with default type' do
      cols = ['col1', 'col2', 'col3']
      @dwh.create_table(@random_table_name, cols)
      @created_tables = [@random_table_name]

      # table exists
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # cols are the same
      expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})
    end

    it "doesn't create a table when it already exists" do
      cols = ['col1', 'col2', 'col3']
      cols2 = ['col1', 'col2']
      @dwh.create_table(@random_table_name, cols)
      @created_tables = [@random_table_name]

      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # try to create a table with di
      @dwh.create_table(@random_table_name, cols2, skip_if_exists: true)

      # table still exists
      expect(@dwh.table_exists?(@random_table_name)).to eq true
      # cols are the same
      expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})
    end

    it 'creates a table with given types' do
      cols = [
        {
          column_name: 'col1',
          data_type: 'varchar(88)'
        }, {
          column_name: 'col2',
          data_type: 'int'
        }, {
          column_name: 'col3',
          data_type: 'boolean'
        }
      ]
      @dwh.create_table(@random_table_name, cols)
      @created_tables = [@random_table_name]

      # table exists
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # cols are the same
      expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(cols)
    end
  end

  describe '#drop_table' do
    it 'drops a table' do
      cols = ['col1', 'col2', 'col3']

      @dwh.create_table(@random_table_name, cols)
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # it shouldn't exist after being dropped
      @dwh.drop_table(@random_table_name)
      expect(@dwh.table_exists?(@random_table_name)).to eq false
    end
  end

  describe '#rename_table' do
    it 'renames a table' do
      cols = ['col1', 'col2', 'col3']

      @dwh.create_table(@random_table_name, cols)
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # the renamed table should exist, not the old name
      changed_name = "#{@random_table_name}_something"
      @dwh.rename_table(@random_table_name, changed_name)
      expect(@dwh.table_exists?(@random_table_name)).to eq false
      expect(@dwh.table_exists?(changed_name)).to eq true

      @created_tables = [changed_name]
    end
  end

  describe '#csv_to_new_table' do
    it 'creates a new table from csv' do
      path = 'spec/data/bike.csv'
      @dwh.csv_to_new_table(@random_table_name, path)

      # table exists
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # cols are the same as in the csv
      expected_cols = File.open(path, &:gets).strip.split(',')
      expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(expected_cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})
      @created_tables = [@random_table_name]
    end
  end

  describe '#export_table' do
    it 'exports a created table' do
      path = 'spec/data/bike.csv'
      @dwh.csv_to_new_table(@random_table_name, path)

      # table exists
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      # export it
      f = Tempfile.new('bike.csv')
      @dwh.export_table(@random_table_name, f)

      # should be the same except for order of the lines
      imported = Set.new(CSV.read(path))
      exported = Set.new(CSV.read(f))

      expect(exported).to eq imported
      @created_tables = [@random_table_name]
    end
  end

  describe '#load_data_from_csv' do
    it 'loads data from csv to existing table' do
      path = 'spec/data/bike.csv'

      # create the table
      @dwh.create_table_from_csv_header(@random_table_name, path)
      expect(@dwh.table_exists?(@random_table_name)).to eq true

      expected_cols = File.open(path, &:gets).strip.split(',')
      expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(expected_cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})

      # load the data there
      @dwh.load_data_from_csv(@random_table_name, path)
    end
  end
end