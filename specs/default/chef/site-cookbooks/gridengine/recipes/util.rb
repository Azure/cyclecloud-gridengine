class Chef
  class Resource
    class Service

      # Checks that a process id is active given a filename containing
      # the process id - false if the file isn't found
      def pidfile_running?(pidfile)
        Chef::Log.debug("Checking pidfile: #{pidfile}")
        return false unless ::File.exist? pidfile

        begin
          pid = ::File.read(pidfile).to_i
          ::Process.getpgid(pid)
          Chef::Log.debug("Process #{pid} is already running")
          return true
        rescue
          return false
        end
      end
    end
  end
end
