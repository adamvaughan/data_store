require 'fileutils'
require 'forwardable'

module DataStore
  # A file storing data in the data store. Data files are stored in directories
  # based on the year of the data they contain and the UUID the data is for. The
  # file is stored two levels deep using the first two, then the second two, characters
  # of the UUID as the directory names. The file is then named as the UUID with
  # the day range of the data stored in the file appended to it.
  #
  # For example, to store data for b731a7d0-774b-012f-582a-482a14096e91 from
  # Jan 3, 2012 to Jan 6, 2012, the path to the file would be:
  #
  #     /path/to/data/2012/b7/31/b731a7d0-774b-012f-582a-482a14096e91_003_006
  #
  # All data files start with a header that contains the time range the file contains
  # data for, stored as two integers. Records are stored directly after the header
  # contiguously. Records are stored as the record time as an integer followed
  # by the record value as a float.
  class DataFile
    extend Forwardable

    UUID_LENGTH = 36

    def_delegators self, :base_path, :generate_path, :uuid_to_path

    attr_reader :path

    def initialize(path)
      @path = path
      @basename = File.basename(@path)
      @dirname = File.dirname(@path)
    end

    # Gets a file capable of storing data for the given UUID at the given time.
    # If a file can not be found, a new file is created.
    def self.storing(uuid, time)
      files_for_uuid(uuid, time.year).find do |file|
        return file if file.can_store?(time)
      end

      self.new(generate_path(uuid, time, time))
    end

    # Gets all of the files containing data for the given UUID in the given time
    # range.
    def self.containing(uuid, time_range)
      year_range = time_range.begin.year..time_range.end.year
      files = []

      year_range.each do |year|
        files.concat files_for_uuid(uuid, year).select { |file| file.contains?(time_range) }
      end

      files
    end

    def self.uuid_to_path(uuid)
      File.join(uuid[0..1], uuid[2..3], uuid)
    end

    # Gets the path to the file for storing data in the given year with the given
    # UUID without the day range appended.
    def self.base_path(uuid, year)
      File.join(DataStore.config.data_directory, year.to_s, uuid_to_path(uuid))
    end

    # Generates a path to store data for the given UUID and time range. If the
    # initial file name already exists, an index is appended until a free file
    # name is found.
    def self.generate_path(uuid, start_time, end_time)
      path = "#{base_path(uuid, start_time.year)}_#{'%03d' % start_time.yday}_#{'%03d' % end_time.yday}"
      original_path = path
      index = 1

      while File.exists?(path)
        path = "#{original_path}_#{index}"
        index += 1
      end

      path
    end

    # Gets the files containing data for the given UUID in the given year.
    def self.files_for_uuid(uuid, year)
      Dir[base_path(uuid, year) + '*'].map { |path| self.new(path) }
    end

    # Extracts the UUID from the file name.
    def uuid
      @uuid ||= @basename[0...UUID_LENGTH]
    end

    # Extracts the start day from the file name.
    def start_day
      @start_day ||= @basename[37..39].to_i
    end

    # Reads the start time from the file header.
    def start_time
      @start_time ||= Time.at(read(4, 0).unpack('L').first)
    end

    # Extracts the end day from the file name.
    def end_day
      @end_day ||= @basename[41..43].to_i
    end

    # Reads the end time from the file header.
    def end_time
      @end_time ||= Time.at(read(4, 4).unpack('L').first)
    end

    # Gets the records from the file. If a block is given, each record is yielded
    # to it. Otherwise, an enumerator is returned.
    def records
      Enumerator.new do |y|
        seek 8

        loop do
          begin
            y << Record.new(*read(8).unpack('LF'))
          rescue
            break
          end
        end
      end
    end

    # Appends a record to the file and updates the file header.
    def <<(record)
      if File.exists?(@path)
        write [record.time.to_i].pack('L'), 4
      else
        FileUtils.mkdir_p(@dirname)
        FileUtils.touch(@path)
        write [record.time.to_i, record.time.to_i].pack('LL'), 0
      end

      write [record.time.to_i, record.value].pack('LF'), 0, IO::SEEK_END
      @end_time = record.time

      self
    end

    alias_method :add, :<<

    # Determines if a record at the given time can be stored in the file.
    def can_store?(time)
      if end_day < time.yday || (end_day == time.yday && end_time <= time)
        (time.yday - start_day) <= DataStore.config.max_days_per_file
      end
    end

    # Determines if the file already contains the maximum number of days per file.
    def full?
    end

    # Determines if the file contains any data in the given time range.
    def contains?(time_range)
      return false if start_day > time_range.end.yday || end_day < time_range.begin.yday
      return false if start_day == time_range.begin.yday && start_time > time_range.end
      return false if end_day == time_range.end.yday && end_time < time_range.begin
      true
    end

    # Closes the file and renames it to match the range of data it contains, if
    # needed.
    def close
      if @file
        start_time_day = start_time.yday
        end_time_day = end_time.yday

        @file.close if @file
        @file = nil

        unless start_day == start_time_day && end_day == end_time_day
          path = generate_path(uuid, start_time, end_time)
          File.rename(@path, path)
          @path = path
          @basename = File.basename(@path)
        end
      end
    end

    def <=>(other)
      path <=> other.path
    end

    def ==(other)
      path == other.path
    end

    private

    def open
      @file ||= File.open(@path, 'r+b')
    end

    def read(length, offset = nil, whence = IO::SEEK_SET)
      open
      @file.seek(offset, whence) if offset
      @file.read length
    end

    def write(data, offset = nil, whence = IO::SEEK_SET)
      open
      @file.seek(offset, whence) if offset
      bytes = @file.syswrite data
    end

    def seek(offset, whence = IO::SEEK_SET)
      open
      @file.seek offset, whence
    end
  end
end
