require 'eventmachine'

require 'data_store/configuration'
require 'data_store/server'
require 'data_store/connection'
require 'data_store/request'
require 'data_store/data_file'
require 'data_store/record'

module DataStore
  def self.config
    @config ||= Configuration.new
  end
end
