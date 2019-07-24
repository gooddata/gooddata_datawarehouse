require 'tempfile'
require 'gooddata_datawarehouse/datawarehouse'

CSV_PATH = 'spec/data/bike.csv'
CSV_PATH2 = 'spec/data/bike2.csv'
WRONG_CSV_PATH = 'spec/data/wrong-bike.csv'
EMPTY_HEADER_CSV_PATH = 'spec/data/emptyheader-bike.csv'
CSV_REGEXP = 'spec/data/bike*.csv'

SST_TOKEN = ENV['GDC_SST']

class Helper
  def self.create_default_connection
    GoodData::Datawarehouse.new(ENV['USERNAME'], ENV['PASSWORD'], ENV['INSTANCE_ID'])
  end

  def self.create_connection_with_sst
    GoodData::Datawarehouse.new_instance(:jdbc_url => ENV['JDBC_URL'], :sst => SST_TOKEN)
  end

  def self.line_count(f)
    i = 0
    CSV.foreach(f, :headers => true) {|_| i += 1}
    i
  end
end

describe GoodData::Datawarehouse do
  describe "dwh instance creation" do
    it "creates an instance with custom jdbc url" do
      if SST_TOKEN.to_s.empty?
        dwh = GoodData::Datawarehouse.new(ENV['USERNAME'], ENV['PASSWORD'], nil, :jdbc_url => "jdbc:dss://secure.gooddata.com/gdc/dss/instances/#{ENV['INSTANCE_ID']}")
        expect(dwh.table_exists?('hahahaha')).to eq false
      else
        dwh = Helper::create_connection_with_sst
        expect(dwh.table_exists?('hahahaha')).to eq false
      end
    end
  end

  describe 'Table operations' do
    before(:each) do
      if SST_TOKEN.to_s.empty?
        @dwh = Helper::create_default_connection
      else
        @dwh = Helper::create_connection_with_sst
      end
      @random = rand(10000000).to_s
      @random_table_name = "temp_#{@random}"
      @created_tables = nil
    end

    after(:each) do
      @created_tables ||= [@random_table_name]
      @created_tables.each{|t| @dwh.drop_table(t) if t} if @created_tables
    end

    describe '#create_table' do
      it 'creates a table with default type' do
        cols = ['col1', 'col2', 'col3']
        @dwh.create_table(@random_table_name, cols)

        # table exists
        expect(@dwh.table_exists?(@random_table_name)).to eq true

        # cols are the same
        expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})
      end

      it "doesn't create a table when it already exists" do
        cols = ['col1', 'col2', 'col3']
        cols2 = ['col1', 'col2']
        @dwh.create_table(@random_table_name, cols)

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

        @random_table_name = nil
      end
    end

    def check_cols
      # cols are the same as in the csv
      expected_cols = File.open(CSV_PATH, &:gets).strip.split(',')
      expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(expected_cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})
    end

    def check_table_exists
      # table exists
      expect(@dwh.table_exists?(@random_table_name)).to eq true
    end

    def check_row_count(files=[CSV_PATH, CSV_PATH2])
      expected_count = files.map {|f| Helper.line_count(f)}.reduce(:+)
      # there are lines from both of the csvs
      expect(@dwh.table_row_count(@random_table_name)).to eq expected_count
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
        @dwh.csv_to_new_table(@random_table_name, CSV_PATH)

        # table exists
        expect(@dwh.table_exists?(@random_table_name)).to eq true

        # cols are the same as in the csv
        check_cols
      end


      it "loads all files in a directory, in paralel" do
        # make a tempdir and copy the csvs there
        Dir.mktmpdir('foo') do |dir|
          FileUtils.cp(CSV_PATH, dir)
          FileUtils.cp(CSV_PATH2, dir)

          @dwh.csv_to_new_table(@random_table_name, dir, :paralel_copy_thread_count => 2)
        end

        check_table_exists
        check_cols
        check_row_count
      end

      it "loads all files given in a list" do
        @dwh.csv_to_new_table(@random_table_name, [CSV_PATH, CSV_PATH2])

        check_table_exists
        check_cols
        check_row_count
      end

      it "loads all files given by a regexp" do
        @dwh.csv_to_new_table(@random_table_name, CSV_REGEXP)

        check_table_exists
        check_cols
        check_row_count
      end

      it 'writes exceptions and rejections to files at given path, passed strings' do
        rej = Tempfile.new('rejections.csv')
        exc = Tempfile.new('exceptions.csv')

        @dwh.csv_to_new_table(@random_table_name, CSV_PATH, :exceptions_file => exc.path, :rejections_file => rej.path)

        expect(File.size(rej)).to eq 0
        expect(File.size(exc)).to eq 0
      end

      it 'overwrites the rejections and exceptions' do
        rej = Tempfile.new('rejections.csv')
        exc = Tempfile.new('exceptions.csv')

        @dwh.csv_to_new_table(@random_table_name, WRONG_CSV_PATH, :exceptions_file => exc.path, :rejections_file => rej.path, :ignore_parse_errors => true)

        rej_size = File.size(rej)
        exc_size = File.size(exc)

        expect(rej_size).to be > 0
        expect(exc_size).to be > 0

        # load it again and see if it was overwritten - has the same size
        @dwh.load_data_from_csv(@random_table_name, WRONG_CSV_PATH, :exceptions_file => exc.path, :rejections_file => rej.path, :ignore_parse_errors => true)

        expect(File.size(rej)).to eq rej_size
        expect(File.size(exc)).to be exc_size
      end

      it 'writes exceptions and rejections to files at given path, passed files' do
        rej = Tempfile.new('rejections.csv')
        exc = Tempfile.new('exceptions.csv')

        @dwh.csv_to_new_table(@random_table_name, CSV_PATH, :exceptions_file => exc, :rejections_file => rej)

        expect(File.size(rej)).to eq 0
        expect(File.size(exc)).to eq 0
      end

      it "writes exceptions and rejections to files at given absolute path, when it's wrong there's something" do
        rej = Tempfile.new('rejections.csv')
        exc = Tempfile.new('exceptions.csv')

        @dwh.csv_to_new_table(@random_table_name, WRONG_CSV_PATH, :exceptions_file => exc.path, :rejections_file => rej.path, :ignore_parse_errors => true)

        expect(File.size(rej)).to be > 0
        expect(File.size(exc)).to be > 0
      end

      it "writes exceptions and rejections to files at given relative path, when it's wrong there's something" do
        rej = Tempfile.new('rejections.csv')
        exc = Tempfile.new('exceptions.csv')

        if File.dirname(rej) != File.dirname(exc)
          raise "two directories for tempfiles!"
        end

        csv_path = File.expand_path(WRONG_CSV_PATH)

        Dir.chdir(File.dirname(rej)) do
          @dwh.csv_to_new_table(@random_table_name, csv_path, :exceptions_file => File.basename(exc), :rejections_file => File.basename(rej), :ignore_parse_errors => true)
        end


        expect(File.size(rej)).to be > 0
        expect(File.size(exc)).to be > 0
      end

      it "loads fine when ignoring errors and not passing files" do
        @dwh.csv_to_new_table(@random_table_name, CSV_PATH, :ignore_parse_errors => true)

        # table exists
        expect(@dwh.table_exists?(@random_table_name)).to eq true

        # cols are the same as in the csv
        expected_cols = File.open(CSV_PATH, &:gets).strip.split(',')
        expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(expected_cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})
      end

      it "works with non-existing rejection/exception files" do
        t = Tempfile.new('haha')
        d = File.dirname(t)

        rej = File.join(d, @random_table_name + '_rej')
        exc = File.join(d, @random_table_name + '_exc')

        expect(File.exists?(rej)).to be false
        expect(File.exists?(exc)).to be false

        @dwh.csv_to_new_table(@random_table_name, WRONG_CSV_PATH, :exceptions_file => exc, :rejections_file => rej, :ignore_parse_errors => true)

        expect(File.size(rej)).to be > 0
        expect(File.size(exc)).to be > 0
      end

      it "fails if one of the files is wrong" do
        expect{@dwh.csv_to_new_table(@random_table_name, [CSV_PATH, WRONG_CSV_PATH])}.to raise_error(ArgumentError)
      end

      it "creates exceptions / rejections for each file when wanted" do
        rej = Tempfile.new('rejections.csv')
        exc = Tempfile.new('exceptions.csv')

        @dwh.csv_to_new_table(@random_table_name, [CSV_PATH, WRONG_CSV_PATH], :exceptions_file => exc.path, :rejections_file => rej.path, :ignore_parse_errors => true)

        expect(File.size("#{rej.path}-#{File.basename(WRONG_CSV_PATH)}")).to be > 0
        expect(File.size("#{exc.path}-#{File.basename(WRONG_CSV_PATH)}")).to be > 0
      end
      it "creates empty1, etc. columns for empty header columns" do
        @dwh.csv_to_new_table(@random_table_name, EMPTY_HEADER_CSV_PATH)
        # it should have cols empty1,2
        expect(@dwh.get_columns(@random_table_name).map {|c| c[:column_name]}).to include('empty1', 'empty2')
      end
    end

    describe '#export_table' do
      it 'exports a created table' do
        @dwh.csv_to_new_table(@random_table_name, CSV_PATH)

        # table exists
        expect(@dwh.table_exists?(@random_table_name)).to eq true

        # export it
        f = Tempfile.new('bike.csv')
        @dwh.export_table(@random_table_name, f)

        # should be the same except for order of the lines
        imported = Set.new(CSV.read(CSV_PATH))
        exported = Set.new(CSV.read(f))

        expect(exported).to eq imported
      end
    end

    describe '#load_data_from_csv' do
      it 'loads data from csv to existing table' do
        # create the table
        @dwh.create_table_from_csv_header(@random_table_name, CSV_PATH)
        expect(@dwh.table_exists?(@random_table_name)).to eq true

        expected_cols = File.open(CSV_PATH, &:gets).strip.split(',')
        expect(Set.new(@dwh.get_columns(@random_table_name))).to eq Set.new(expected_cols.map {|c| {:column_name => c, :data_type => GoodData::SQLGenerator::DEFAULT_TYPE}})

        # load the data there
        @dwh.load_data_from_csv(@random_table_name, CSV_PATH)

        # export it
        f = Tempfile.new('bike.csv')
        @dwh.export_table(@random_table_name, f)

        # should be the same except for order of the lines
        imported = Set.new(CSV.read(CSV_PATH))
        exported = Set.new(CSV.read(f))

        expect(exported).to eq imported
      end

      it "can load multiple files" do
        # create the table
        @dwh.create_table_from_csv_header(@random_table_name, CSV_PATH)
        check_table_exists
        check_cols

        # load the data there
        @dwh.load_data_from_csv(@random_table_name, [CSV_PATH, CSV_PATH2])

        check_row_count
      end

      it 'fails for a wrong csv' do
        # create the table
        @dwh.create_table_from_csv_header(@random_table_name, WRONG_CSV_PATH)
        expect(@dwh.table_exists?(@random_table_name)).to eq true

        # load the data there - expect fail
        expect{@dwh.load_data_from_csv(@random_table_name, WRONG_CSV_PATH)}.to raise_error(ArgumentError)
      end

      it 'truncates the data that is already there' do
        @dwh.create_table_from_csv_header(@random_table_name, CSV_PATH)
        check_table_exists
        check_cols

        # load the data there
        @dwh.load_data_from_csv(@random_table_name, CSV_PATH)
        check_row_count([CSV_PATH])

        # load the data there again, count should stay
        @dwh.load_data_from_csv(@random_table_name, CSV_PATH2)
        check_row_count([CSV_PATH2])
      end

      it "keeps the data that is there if append option passed" do
        @dwh.create_table_from_csv_header(@random_table_name, CSV_PATH)
        check_table_exists
        check_cols

        # load the data there
        @dwh.load_data_from_csv(@random_table_name, CSV_PATH)
        check_row_count([CSV_PATH])

        # append the data
        @dwh.load_data_from_csv(@random_table_name, CSV_PATH2, :append => true)
        check_row_count([CSV_PATH, CSV_PATH2])
      end
    end

    describe "#truncate_table" do
      it "truncates the given table" do
        @dwh.csv_to_new_table(@random_table_name, CSV_PATH)
        @dwh.truncate_table(@random_table_name)
        expect(@dwh.table_row_count(@random_table_name)).to eq 0
      end
    end

    describe '#get_columns' do
      it 'gives you the right list of columns' do
        expected_cols = File.open(CSV_PATH, &:gets).strip.split(',')
        @dwh.create_table_from_csv_header(@random_table_name, CSV_PATH)
      end
      # TODO more tests
    end
  end
end