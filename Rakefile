require 'coveralls/rake/task'
require 'rake/testtask'
require 'rspec/core/rake_task'

Coveralls::RakeTask.new

task default: %w[ci]

desc 'Run continuous integration test'
task :ci do
  Rake::Task['test:unit'].invoke
  Rake::Task['coveralls:push'].invoke
end

namespace :test do
  desc "Run unit tests"
  RSpec::Core::RakeTask.new(:unit) do |t|
    t.pattern = 'spec/**/*.rb'
  end
end