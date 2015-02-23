#!/bin/sh

# get the new version
VERSION=`bundle exec ruby <<-EORUBY

  require 'gooddata_datawarehouse'
  puts GoodData::Datawarehouse::VERSION

EORUBY`

# create tag and push it
TAG="v$VERSION"
git tag $TAG
git push origin $TAG

# build and push the gem
gem build gooddata_datawarehouse.gemspec
gem push "gooddata_datawarehouse-$VERSION.gem"

# update the gem after a few secs
echo "Sleeping.."
sleep 30
gem update gooddata_datawarehouse