module DataStore
  # A record stored in a data file.
  class Record
    attr_accessor :time, :value

    def initialize(time = nil, value = nil)
      @time = Time.at(time.to_i)
      @value = value
    end

    def <=>(other)
      time <=> other.time
    end

    def ==(other)
      time == other.time && value == other.value
    end
  end
end
