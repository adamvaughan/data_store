$:.push File.expand_path('../../lib', __FILE__)

require 'data_store'

module Helpers
  def request_data(type, options = {})
    options = { :uuid => 'b731a7d0-774b-012f-582a-482a14096e91',
                :record_count => 1 }.merge(options)

    data = case type
           when :get then [1].pack('L')
           when :put then [2].pack('L')
           else [0].pack('L')
           end

    data << options[:uuid]
    data << [0].pack('L')
    data << [options[:record_count]].pack('L')

    if type == :get
      data << [(options[:start_time] || 0).to_i].pack('L')
      data << [(options[:end_time] || 0).to_i].pack('L')
    elsif type == :put
      data << [(options[:record_time] || 0).to_i].pack('L')
      data << [options[:record_value] || 0.0].pack('F')
    else
      data << [0, 0].pack('LL')
    end

    data
  end

  def response_data(count)
    [count].pack('L')
  end
end

RSpec.configure do |config|
  config.include Helpers
end

