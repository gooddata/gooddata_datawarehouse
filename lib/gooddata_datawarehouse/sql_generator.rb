module GoodData
  class SQLGenerator
    DEFAULT_TYPE = 'VARCHAR(1023)'
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
          c.is_a?(String) ? "#{c} #{DEFAULT_TYPE}" : "#{c[:name]} #{c[:type] || DEFAULT_TYPE}"
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
    end
  end
end