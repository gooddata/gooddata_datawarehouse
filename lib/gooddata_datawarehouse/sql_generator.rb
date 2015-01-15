module GoodData
  class SQLGenerator
    DEFAULT_TYPE = 'varchar(1023)'
    class << self

      def rename_table(old_name, new_name)
        "ALTER TABLE #{old_name} RENAME TO #{new_name}"
      end

      def drop_table(table_name, opts={})
        "DROP TABLE #{opts[:skip_if_exists] ? 'IF EXISTS' : ''} #{table_name}"
      end

      def create_table(table_name, columns, opts={})
        not_exists = opts[:skip_if_exists] ? 'IF NOT EXISTS' : ''
        columns_string = columns.map { |c|
          c.is_a?(String) ? "#{c} #{DEFAULT_TYPE}" : "#{c[:column_name]} #{c[:data_type] || DEFAULT_TYPE}"
        }.join(', ')
        "CREATE TABLE #{not_exists} #{table_name} (#{columns_string})"
      end

      def load_data(table, csv, columns, opts={})
        col_list = columns.join(', ')
        skip = opts[:no_header] ? '' : 'SKIP 1'
        parser = opts[:parser] || 'GdcCsvParser()'
        escape_as = opts[:escape_as] || '"'

        exc_rej = if opts[:ignore_parse_errors] && opts[:exceptions_file].nil? && opts[:rejections_file].nil?
                    ''
                  else
                    "EXCEPTIONS '#{File.absolute_path(opts[:exceptions_file])}' REJECTED DATA '#{File.absolute_path(opts[:rejections_file])}'"
                  end

        %Q{COPY #{table} (#{col_list})
        FROM LOCAL '#{File.absolute_path(csv)}' WITH PARSER #{parser}
        ESCAPE AS '#{escape_as}'
        #{skip}
        #{exc_rej}}
      end

      def get_table_count(table_name)
        "SELECT COUNT(*) FROM tables WHERE table_name = '#{table_name}'"
      end

      def get_columns(table_name)
        "SELECT column_name, data_type FROM columns WHERE table_name = '#{table_name}'"
      end

      def select_all(table_name, opts={})
        limit = opts[:limit] ? "LIMIT #{opts[:limit]}" : ''
        "SELECT * FROM #{table_name} #{limit}"
      end
    end
  end
end