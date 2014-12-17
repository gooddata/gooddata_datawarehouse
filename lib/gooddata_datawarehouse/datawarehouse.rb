require 'jdbc/dss'
require 'sequel'
require 'logger'

require_relative 'sql_generator'

module GoodData
  class Datawarehouse
    def initialize(username, password, instance_id, options={})
      @logger = Logger.new(STDOUT)
      @username = username
      @password = password
      @jdbc_url = "jdbc:dss://secure.gooddata.com/gdc/dss/instances/#{instance_id}"
      Jdbc::DSS.load_driver
      Java.com.gooddata.dss.jdbc.driver.DssDriver
    end

    # TODO export_table https://gist.github.com/cvengros/32ad1a518b3956617522

    def rename_table(old_name, new_name)
      execute(GoodData::SQLGenerator.rename_table(old_name, new_name))
    end

    def drop_table(table_name, opts={})
      execute(GoodData::SQLGenerator.drop_table(table_name,opts))
    end

    def csv_to_new_table(table_name, csv_path, opts={})
      cols = create_table_from_csv_header(table_name, csv_path, opts)
      load_data_from_csv(table_name, cols, opts={})
    end

    def load_data_from_csv(table_name, csv_path, opts={})
      columns = opts[:columns] || get_csv_headers(csv_path)
      execute(GoodData::SQLGenerator.load_data(table_name, csv_path, columns))
    end

    # returns a list of columns created
    # does nothing if file empty, returns []
    def create_table_from_csv_header(table_name, csv_path, opts={})
      # take the header as a list of columns
      columns = get_csv_headers(csv_path)
      create_table(table_name, columns, opts) unless columns.empty?
      columns
    end

    def create_table(name, columns, options={})
      execute(GoodData::SQLGenerator.create_table(name, columns, options))
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
    def execute_select(sql, fetch_handler=nil, count=false)
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

        # go throug the rows returned and call the block
        return f.each do |row|
          yield(row)
        end
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
