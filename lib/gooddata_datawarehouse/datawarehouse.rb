require 'jdbc/dss'
require 'sequel'
require 'logger'
require 'csv'
require 'tempfile'

require_relative 'sql_generator'

module GoodData
  class Datawarehouse
    def initialize(username, password, instance_id, opts={})
      @logger = Logger.new(STDOUT)
      @username = username
      @password = password
      @jdbc_url = "jdbc:dss://secure.gooddata.com/gdc/dss/instances/#{instance_id}"
      if @username.nil? || @password.nil? || instance_id.nil?
        fail ArgumentError, "username, password and/or instance_id are nil. All of them are mandatory."
      end

      Jdbc::DSS.load_driver
      Java.com.gooddata.dss.jdbc.driver.DssDriver
    end

    def export_table(table_name, csv_path)
      CSV.open(csv_path, 'wb', :force_quotes => true) do |csv|
        # get the names of cols
        cols = get_columns(table_name).map {|c| c[:column_name]}

        # write header
        csv << cols

        # get the keys for columns, stupid sequel
        col_keys = nil
        rows = execute_select(GoodData::SQLGenerator.select_all(table_name, limit: 1))

        col_keys = rows[0].keys

        execute_select(GoodData::SQLGenerator.select_all(table_name)) do |row|
          # go through the table write to csv
          csv << row.values_at(*col_keys)
        end
      end
    end

    def rename_table(old_name, new_name)
      execute(GoodData::SQLGenerator.rename_table(old_name, new_name))
    end

    def drop_table(table_name, opts={})
      execute(GoodData::SQLGenerator.drop_table(table_name,opts))
    end

    def csv_to_new_table(table_name, csvs, opts={})
      csv_list = list_files(csvs)
      cols = create_table_from_csv_header(table_name, csv_list[0], opts)
      load_data_from_csv(table_name, csv_list, opts.merge(columns: cols))
    end

    def load_data_from_csv(table_name, csvs, opts={})
      # get the list of files to load and columns in the csv
      csv_list = list_files(csvs)
      columns = opts[:columns] || get_csv_headers(csv_list[0])

      # load each csv from the list
      single_file = (csv_list.size == 1)

      csv_list.each do |csv_path|
        if opts[:ignore_parse_errors] && opts[:exceptions_file].nil? && opts[:rejections_file].nil?
          exc = nil
          rej = nil
          opts_file = opts
        else
          opts_file = opts.clone
          # priradit do opts i do exc -
          # temporary files to get the excepted records (if not given)
          exc = opts_file[:exceptions_file] = init_file(opts_file[:exceptions_file], 'exceptions', csv_path, single_file)
          rej = opts_file[:rejections_file] = init_file(opts_file[:rejections_file], 'rejections', csv_path, single_file)
        end

        # execute the load
        execute(GoodData::SQLGenerator.load_data(table_name, csv_path, columns, opts_file))

        exc.close if exc
        rej.close if rej

        # if there was something rejected and it shouldn't be ignored, raise an error
        if ((exc && File.size?(exc)) || (rej && File.size?(rej))) && (! opts[:ignore_parse_errors])
          fail ArgumentError, "Some lines in the CSV didn't go through. Exceptions: #{IO.read(exc)}\nRejected records: #{IO.read(rej)}"
        end
      end
    end

    def init_file(given_filename, key, csv_path, single_file)
      # only use file postfix if there are multiple files
      postfix = single_file ? '' : "-#{File.basename(csv_path)}"

      # take what we have and put the source csv name at the end
      given_filename = given_filename.path if given_filename.is_a?(File)
      f = "#{given_filename || Tempfile.new(key).path}#{postfix}"
      f = File.new(f, 'w') unless f.is_a?(File)
      f
    end

    # returns a list of columns created
    # does nothing if file empty, returns []
    def create_table_from_csv_header(table_name, csv_path, opts={})
      # take the header as a list of columns
      columns = get_csv_headers(csv_path)
      create_table(table_name, columns, opts) unless columns.empty?
      columns
    end

    def create_table(name, columns, opts={})
      execute(GoodData::SQLGenerator.create_table(name, columns, opts))
    end

    def table_exists?(name)
      count = execute_select(GoodData::SQLGenerator.get_table_count(name), :count => true)
      count > 0
    end

    def table_row_count(table_name)
      execute_select(GoodData::SQLGenerator.get_row_count(table_name), :count => true)
    end

    def get_columns(table_name)
      res = execute_select(GoodData::SQLGenerator.get_columns(table_name))
    end

    # execute sql, return nothing
    def execute(sql_strings)
      if ! sql_strings.kind_of?(Array)
        sql_strings = [sql_strings]
      end
      connect do |connection|
        sql_strings.each do |sql|
          @logger.info("Executing sql: #{sql}") if @logger
          connection.run(sql)
        end
      end
    end

    # executes sql (select), for each row, passes execution to block
    def execute_select(sql, opts={})
      fetch_handler = opts[:fetch_handler]
      count = opts[:count]

      connect do |connection|
        # do the query
        f = connection.fetch(sql)

        @logger.info("Executing sql: #{sql}") if @logger
        # if handler was passed call it
        if fetch_handler
          fetch_handler.call(f)
        end

        if count
          return f.first[:count]
        end

        # if block given yield to process line by line
        if block_given?
          # go through the rows returned and call the block
          return f.each do |row|
            yield(row)
          end
        end

        # return it all at once
        f.map{|h| h}
      end
    end

    def connect
      Sequel.connect @jdbc_url,
        :username => @username,
        :password => @password do |connection|
          yield(connection)
      end
    end

    private

    # returns an array of file paths (strings)
    def list_files(csvs)
      # csvs can be:
      case csvs
        when String

          # directory
          if File.directory?(csvs)
            return (Dir.entries(csvs) - ['.', '..']).map{|f| File.join(csvs, f)}
          end

          # filename or pattern
          return Dir.glob(csvs)

        # array
        when Array
          return csvs
      end
    end

    def get_csv_headers(csv_path)
      header_str = File.open(csv_path, &:gets)
      if header_str.nil? || header_str.empty?
        return []
      end
      header_str.split(',').map{ |s| s.gsub(/[\s"-]/,'') }
    end
  end
end
