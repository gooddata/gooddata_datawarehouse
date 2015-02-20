# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_datawarehouse/version'

if RUBY_PLATFORM != 'java'
  fail "Only java platform supported! Use jRuby, e.g. rvm use jruby"
end

Gem::Specification.new do |spec|
  spec.name          = "gooddata_datawarehouse"
  spec.version       = GoodData::Datawarehouse::VERSION
  spec.authors       = ["Petr Cvengros"]
  spec.email         = ["petr.cvengros@gooddata.com"]
  spec.summary       = %q{Convenient work with GoodData's Datawarehouse (ADS) }
  spec.description   = ""
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.3"
  spec.add_development_dependency 'rspec', '~>2.14'
  spec.add_development_dependency 'pry', '~> 0.9'
  spec.add_development_dependency 'coveralls', '~> 0.7', '>= 0.7.0'

  spec.add_dependency "sequel", "~> 4.17"
  spec.add_dependency "gooddata-dss-jdbc", "~> 0.1"
  spec.add_dependency "pmap", "~> 1.0"
end
