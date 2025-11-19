require "./certificate_provider"

module LavinMQ
  module TrustStoreProvider
    class FilesystemProvider < CertificateProvider
      def initialize(@directory : String)
        raise ArgumentError.new("Directory does not exist: #{@directory}") unless Dir.exists?(@directory)
      end

      def name : String
        "filesystem:#{@directory}"
      end

      def load_certificates : Array(String)
        certificates = Array(String).new

        Dir.each_child(@directory) do |filename|
          # Only process .pem and .crt files
          next unless filename.ends_with?(".pem") || filename.ends_with?(".crt")

          path = File.join(@directory, filename)
          next unless File.file?(path)

          begin
            content = File.read(path)
            # Just return raw PEM content, no parsing needed
            certificates << content
            Log.debug { "Loaded certificate from #{filename}" }
          rescue ex : File::NotFoundError
            Log.warn { "Certificate file disappeared: #{filename}" }
          rescue ex : IO::Error
            Log.warn { "Failed to read certificate from #{filename}: #{ex.message}" }
          end
        end

        Log.info { "Loaded #{certificates.size} certificates from #{@directory}" }
        certificates
      end
    end
  end
end
