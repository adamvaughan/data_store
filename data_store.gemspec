# -*- encoding: utf-8 -*-

$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |s|
  s.name        = 'data_store'
  s.version     = '0.0.1'
  s.authors     = ['Adam Vaughan']
  s.email       = ['adamjvaughan@gmail.com']
  s.homepage    = 'http://github.com/adamvaughan/data_store'
  s.summary     = 'Simple time/value data store based on eventmachine.'
  s.description = s.summary

  s.add_dependency 'eventmachine', '~> 1.0.0'
  s.add_development_dependency 'rspec', '~> 2.11.0'
  s.add_development_dependency 'rake', '~> 0.9.2.2'

  s.files         = Dir['README.md', 'Rakefile', 'Gemfile', 'bin/*', 'lib/**/*']
  s.test_files    = Dir['spec/**/*']
  s.executables   = ['data_store']
  s.require_path = 'lib'
end
