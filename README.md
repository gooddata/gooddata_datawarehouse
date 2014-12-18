# GooddataDatawarehouse

A little library to help you work with GoodData's Datawarehouse

## Installation

You need to run **jRuby** to use this gem, the gem won't work on any other Ruby platform than jRuby. That's because there's a dependency on the JDBC driver

If you're using [rvm](https://rvm.io/rvm/install) (recommended), run:
    
    $ rvm use jruby

If you don't have jruby yet, run

    $ rvm install jruby

Add this line to your application's Gemfile:

```ruby
gem 'gooddata_datawarehouse'
```

And then install:

    $ bundle install

Or install it yourself as:

    $ gem install gooddata_datawarehouse

## Usage

```ruby
require 'gooddata_datawarehouse'

# connect
dwh = GoodData::Datawarehouse.new('you@gooddata.com', 'yourpass', 'your ADS instance id'

# import a csv
dwh.csv_to_new_table('my_table', 'path/to/my.csv')

dwh.table_exists?('my_table') # true
dwh.get_columns('my_table') # [{column_name: 'col1', data_type: 'varchar(88)'}, {column_name: 'col2', data_type: 'int'}]

# run an aribrary sql
dwh.execute('ALTER TABLE my_table ADD COLUMN col3 INTEGER')

# run a select and process results 
dwh.execute_select('SELECT * FROM my_table ORDER BY col1') do |row| 
  puts row[:col1] 
end

# rename a table
dwh.rename_table('my_table', 'my_new_table')

# export to csv
dwh.export_table('my_new_table', 'path/to/my_new.csv')

# drop table
dwh.drop_table('my_new_table')
```


## Contributing

1. Fork it ( https://github.com/[my-github-username]/gooddata_datawarehouse/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
