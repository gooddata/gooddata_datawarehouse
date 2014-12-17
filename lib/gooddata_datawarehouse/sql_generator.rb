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

      def load_data(table, csv, columns)
        col_list = columns.join(', ')

        # TODO: exceptions, rejections
        #       EXCEPTIONS '#{except_filename(filename)}'
        # REJECTED DATA '#{reject_filename(filename)}' }

        %Q{COPY #{table} (#{col_list})
        FROM LOCAL '#{csv}' WITH PARSER GdcCsvParser()
        ESCAPE AS '"'
         SKIP 1}
      end

      def get_table_count(table_name)
        "SELECT COUNT(*) FROM tables WHERE table_name = '#{table_name}'"
      end

      def get_columns(table_name)
        "SELECT column_name, data_type FROM columns WHERE table_name = '#{table_name}'"
      end

      def select_all(table_name, options={})
        limit = options[:limit] ? "LIMIT #{options[:limit]}" : ''
        "SELECT * FROM #{table_name} #{limit}"
      end
    end
  end
end