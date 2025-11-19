module LavinMQ
  module TrustStoreProvider
    # Abstract interface for certificate providers
    # Providers load trusted certificates from different sources
    abstract class CertificateProvider
      Log = LavinMQ::Log.for("trust_store.provider")

      # Return raw PEM content as strings, no need to parse
      abstract def load_certificates : Array(String)
      abstract def name : String
    end
  end
end
