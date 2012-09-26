module DataStore
  class Configuration
    attr_accessor :data_directory, :max_days_per_file

    def initialize
      @max_days_per_file = 30
    end
  end
end
