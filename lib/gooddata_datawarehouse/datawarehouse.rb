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

    def csv_to_new_table(table_name, csv_path, opts={})
      cols = create_table_from_csv_header(table_name, csv_path, opts)
      load_data_from_csv(table_name, csv_path, opts.merge(columns: cols))
    end

    def load_data_from_csv(table_name, csv_path, opts={})
      columns = opts[:columns] || get_csv_headers(csv_path)

      if opts[:ignore_parse_errors] && opts[:exceptions_file].nil? && opts[:rejections_file].nil?
        exc = nil
        rej = nil
      else
        # temporary files to get the excepted records (if not given)
        exc = opts[:exceptions_file] ||= Tempfile.new('exceptions')
        rej = opts[:rejections_file] ||= Tempfile.new('rejections')
        exc = File.new(exc) unless exc.is_a?(File)
        rej = File.new(rej) unless rej.is_a?(File)
      end

      # execute the load
      execute(GoodData::SQLGenerator.load_data(table_name, csv_path, columns, opts))

      exc.close if exc
      rej.close if rej

      # if there was something rejected and it shouldn't be ignored, raise an error
      if ((exc && File.size?(exc)) || (rej && File.size?(rej))) && (! opts[:ignore_parse_errors])
        fail ArgumentError, "Some lines in the CSV didn't go through. Exceptions: #{IO.read(exc)}\nRejected records: #{IO.read(rej)}"
      end
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

    def get_csv_headers(csv_path)
      header_str = File.open(csv_path, &:gets)
      if header_str.nil? || header_str.empty?
        return []
      end
      header_str.split(',').map{ |s| s.gsub(/[\s"-]/,'') }
    end
  end
end
