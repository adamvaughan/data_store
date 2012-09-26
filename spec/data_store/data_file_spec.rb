require 'spec_helper'

module DataStore
  describe DataFile do
    let(:uuid) { 'b731a7d0-774b-012f-582a-482a14096e91' }
    let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_001" }
    let(:file) { DataFile.new(path) }

    before do
      DataStore.config.data_directory = '/tmp/data_store'
      FileUtils.rm_rf('/tmp/data_store')
    end

    describe ".storing" do
      context "when a file capable of storing the data exists" do
        it "returns the file" do
          file.should_receive(:can_store?).and_return(true)
          DataFile.should_receive(:new).with(path).and_return(file)
          Dir.should_receive(:[]).and_return([path])
          DataFile.storing(uuid, Time.at(100121)).should eq(file)
        end
      end

      context "when a file capable of storing the data does not exist" do
        it "returns a new file" do
          other_path = "/tmp/data_store/1970/b7/31/#{uuid}_001_001"
          other_file = DataFile.new(other_path)
          file.should_receive(:can_store?).and_return(false)
          DataFile.should_receive(:new).with(path).and_return(file)
          DataFile.should_receive(:new).with(other_path).and_return(other_file)
          Dir.should_receive(:[]).and_return([path])
          DataFile.storing(uuid, Time.at(100121)).should eq(other_file)
        end
      end
    end

    describe ".containing" do
      it "returns matching files" do
        matching_path = "/tmp/data_store/1970/b7/31/#{uuid}_120_121"
        matching_file = DataFile.new(matching_path)
        matching_file.should_receive(:contains?).and_return(true)
        other_path = "/tmp/data_store/1970/b7/31/#{uuid}_001_001"
        other_file = DataFile.new(other_path)
        other_file.should_receive(:contains?).and_return(false)
        DataFile.should_receive(:new).with(matching_path).and_return(matching_file)
        DataFile.should_receive(:new).with(other_path).and_return(other_file)
        Dir.should_receive(:[]).and_return([matching_path, other_path])
        DataFile.containing(uuid, (Time.at(100121)..Time.at(100122))).should eq([matching_file])
      end
    end

    describe ".uuid_to_path" do
      it "returns the path for the uuid" do
        DataFile.uuid_to_path(uuid).should eq("b7/31/#{uuid}")
      end
    end

    describe ".base_path" do
      it "returns the path without the day range" do
        DataFile.base_path(uuid, 1970).should eq("/tmp/data_store/1970/b7/31/#{uuid}")
      end
    end

    describe ".generate_path" do
      it "returns the path to the file" do
        DataFile.generate_path(uuid, Time.at(100121), Time.at(1000121)).should eq("/tmp/data_store/1970/b7/31/#{uuid}_001_012")
      end

      context "when the file already exists" do
        before do
          FileUtils.mkdir_p(File.dirname(path))
          FileUtils.touch path
        end

        it "generates a file with a suffix" do
          DataFile.generate_path(uuid, Time.at(100121), Time.at(100121)).should eq("/tmp/data_store/1970/b7/31/#{uuid}_001_001_1")
        end
      end
    end

    describe ".files_for_uuid" do
      before do
        FileUtils.mkdir_p(File.dirname(path))
        FileUtils.touch path
        FileUtils.touch "#{path}_1"
        FileUtils.touch path.sub(/e91/, 'a91')
      end

      it "returns files matching the given uuid" do
        files = DataFile.files_for_uuid(uuid, 1970)
        files.should have(2).items
        files.first.path.should eq(path)
        files.last.path.should eq("#{path}_1")
      end
    end

    describe "#uuid" do
      it "returns the uuid for the file" do
        file.uuid.should eq(uuid)
      end
    end

    describe "#start_day" do
      it "returns the start day for the file" do
        path = "/tmp/data_store/1970/b7/31/#{uuid}_120_121"
        file = DataFile.new(path)
        file.start_day.should eq(120)
      end
    end

    describe "#start_time" do
      before do
        FileUtils.mkdir_p(File.dirname(path))
        File.open(path, 'wb') { |file| file.write [100121, 100122].pack('LL') }
      end

      it "returns the start time for the file" do
        file.start_time.should eq(Time.at(100121))
      end
    end

    describe "#end_day" do
      it "returns the end day for the file" do
        path = "/tmp/data_store/1970/b7/31/#{uuid}_120_121"
        file = DataFile.new(path)
        file.end_day.should eq(121)
      end
    end

    describe "#end_time" do
      before do
        FileUtils.mkdir_p(File.dirname(path))
        File.open(path, 'wb') { |file| file.write [100121, 100122].pack('LL') }
      end

      it "returns the end time for the file" do
        file.end_time.should eq(Time.at(100122))
      end
    end

    describe "#records" do
      before do
        FileUtils.mkdir_p(File.dirname(path))
        File.open(path, 'wb') do |file|
          file.write [100121, 100122].pack('LL') # start and end times
          file.write [100121, 0.5].pack('LF') # first record
          file.write [100122, 0.7].pack('LF') # second record
        end
      end

      it "returns an enumerator" do
        file.records.should be_kind_of(Enumerator)
      end

      it "yields each record in the file" do
        records = []
        file.records.each { |record| records << record }
        records.should have(2).items
        records.first.time.should eq(Time.at(100121))
        records.last.time.should eq(Time.at(100122))
      end
    end

    describe "#<<" do
      context "when the file exists" do
        before do
          FileUtils.mkdir_p(File.dirname(path))
          File.open(path, 'wb') do |file|
            file.write [100121, 100122].pack('LL')
            file.write [100121, 0.5].pack('LF')
            file.write [100122, 1.5].pack('LF')
          end
        end

        it "adds the record to the file" do
          record = Record.new(Time.at(100122), 2.5)
          records = []
          file << record
          file.close
          file.records.each { |record| records << record }
          records.should have(3).items
          records.last.should eq(record)
        end
      end

      context "when the file does not exist" do
        it "creates a new file containing the record" do
          record = Record.new(Time.at(100122), 2.5)
          records = []
          file << record
          file.close
          file.records.each { |record| records << record }
          records.should have(1).items
          records.last.should eq(record)
        end
      end
    end

    describe "#can_store?" do
      context "when the file end day is before the record time" do
        it "returns true" do
          path = "/tmp/data_store/1970/b7/31/#{uuid}_010_011"
          file = DataFile.new(path)
          file.can_store?(Time.at(1000201)).should be_true
        end
      end

      context "when the file end day is on the same day as the record time, but before the actual time" do
        let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_011_012" }
        let(:file) { DataFile.new(path) }

        before do
          FileUtils.mkdir_p(File.dirname(path))
          File.open(path, 'w') do |file|
            file.write [1000201, 1000202].pack('LL')
          end
        end

        it "returns true" do
          file.can_store?(Time.at(1000203)).should be_true
        end
      end

      context "when the file end day is on the same day as the record time, but after the actual time" do
        let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_011_012" }
        let(:file) { DataFile.new(path) }

        before do
          FileUtils.mkdir_p(File.dirname(path))
          File.open(path, 'w') do |file|
            file.write [1000201, 1000202].pack('LL')
          end
        end

        it "returns false" do
          file.can_store?(Time.at(1000201)).should be_false
        end
      end

      context "when the file ends after the record time" do
        it "returns false" do
          path = "/tmp/data_store/1970/b7/31/#{uuid}_010_013"
          file = DataFile.new(path)
          file.can_store?(Time.at(1000201)).should be_false
        end
      end

      context "when the record will cause the file to exceed the allowed maximum days per file" do
        it "returns false" do
          DataStore.config.max_days_per_file = 3
          path = "/tmp/data_store/1970/b7/31/#{uuid}_008_011"
          file = DataFile.new(path)
          file.can_store?(Time.at(1000201)).should be_false
        end
      end
    end

    describe "#contains?" do
      context "when the file start day is after the end day of the time range" do
        it "returns false" do
          path = "/tmp/data_store/1970/b7/31/#{uuid}_013_014"
          file = DataFile.new(path)
          file.contains?(Time.at(1000201)..Time.at(1000202)).should be_false
        end
      end

      context "when the file end day is before the start day of the time range" do
        it "returns false" do
          path = "/tmp/data_store/1970/b7/31/#{uuid}_010_011"
          file = DataFile.new(path)
          file.contains?(Time.at(1000201)..Time.at(1000202)).should be_false
        end
      end

      context "when the file start day equals the start day of the time range" do
        context "when the file start time is after the end of the time range" do
          let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_012_013" }
          let(:file) { DataFile.new(path) }

          before do
            FileUtils.mkdir_p(File.dirname(path))
            File.open(path, 'w') do |file|
              file.write [1000203, 1000204].pack('LL')
            end
          end

          it "returns false" do
            file.contains?(Time.at(1000201)..Time.at(1000202)).should be_false
          end
        end
      end

      context "when the file end day equals the end day of the time range" do
        context "when the file end time is before the beginning of the time range" do
          let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_011_012" }
          let(:file) { DataFile.new(path) }

          before do
            FileUtils.mkdir_p(File.dirname(path))
            File.open(path, 'w') do |file|
              file.write [1000200, 1000201].pack('LL')
            end
          end

          it "returns false" do
            file.contains?(Time.at(1000202)..Time.at(1000203)).should be_false
          end
        end

        context "when the file contains data in the time range" do
          let(:path) { "/tmp/data_store/1970/b7/31/#{uuid}_001_012" }
          let(:file) { DataFile.new(path) }

          before do
            FileUtils.mkdir_p(File.dirname(path))
            File.open(path, 'w') do |file|
              file.write [100201, 1000201].pack('LL')
            end
          end

          it "returns true" do
            file.contains?(Time.at(100202)..Time.at(1000202)).should be_true
            file.contains?(Time.at(100202)..Time.at(1000201)).should be_true
            file.contains?(Time.at(100202)..Time.at(1000200)).should be_true
            file.contains?(Time.at(100201)..Time.at(1000202)).should be_true
            file.contains?(Time.at(100201)..Time.at(1000201)).should be_true
            file.contains?(Time.at(100201)..Time.at(1000200)).should be_true
            file.contains?(Time.at(100200)..Time.at(1000202)).should be_true
            file.contains?(Time.at(100200)..Time.at(1000201)).should be_true
            file.contains?(Time.at(100200)..Time.at(1000200)).should be_true
          end
        end
      end
    end

    describe "#close" do
      context "when the changes to the file mandate a name change" do
        context "when the desired file name already exists" do
          before do
            FileUtils.mkdir_p(File.dirname(path))
            FileUtils.touch(path.sub(/_001$/, '_012'))
            FileUtils.touch("#{path.sub(/_001$/, '_012')}_1")
          end

          it "renames the file with a suffix" do
            file << Record.new(100201, 0.5)
            file << Record.new(1000201, 1.5)
            file.close
            File.exists?(path).should be_false
            File.exists?("#{path.sub(/_001$/, '_012')}_1").should be_true
            File.exists?("#{path.sub(/_001$/, '_012')}_2").should be_true
          end
        end

        context "when the desired file name does not exist" do
          it "renames the file" do
            file << Record.new(100201, 0.5)
            file << Record.new(1000201, 1.5)
            file.close
            File.exists?(path).should be_false
            File.exists?(path.sub(/_001$/, '_012')).should be_true
          end
        end
      end
    end

    describe "#<=>" do
      context "when the file path is before the other file path" do
        it "returns -1" do
          first = DataFile.new('/tmp/data_store/file1')
          second = DataFile.new('/tmp/data_store/file2')
          (first <=> second).should eq(-1)
        end
      end

      context "when the file path is after the other file path" do
        it "returns 1" do
          first = DataFile.new('/tmp/data_store/file2')
          second = DataFile.new('/tmp/data_store/file1')
          (first <=> second).should eq(1)
        end
      end

      context "when the file paths are the same" do
        it "returns 0" do
          first = DataFile.new('/tmp/data_store/file1')
          second = DataFile.new('/tmp/data_store/file1')
          (first <=> second).should be_zero
        end
      end
    end

    describe "#==" do
      context "when both files have the same path" do
        it "returns true" do
          first = DataFile.new('path1')
          second = DataFile.new('path1')
          (first == second).should be_true
        end
      end

      context "when the files have different paths" do
        it "returns false" do
          first = DataFile.new('path1')
          second = DataFile.new('path2')
          (first == second).should be_false
        end
      end
    end
  end
end
