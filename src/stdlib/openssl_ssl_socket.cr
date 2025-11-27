# Extension to OpenSSL::SSL::Socket to check verification result
class OpenSSL::SSL::Socket
  # Returns the verification result for the peer certificate
  # 0 (X509_V_OK) means verification succeeded
  def verify_result : LibC::Long
    LibSSL.ssl_get_verify_result(@ssl)
  end

  # Verifies the peer certificate and raises OpenSSL::Error if verification fails
  def verify! : Nil
    result = LibSSL.ssl_get_verify_result(@ssl)
    if result != 0
      error_msg = verify_error_string(result)
      raise OpenSSL::Error.new("Certificate verification failed: #{error_msg}")
    end
  end

  # Returns a human-readable explanation of the verification error
  # ameba:disable Metrics/CyclomaticComplexity
  def verify_error_string(code : LibC::Long) : String
    case code
    when  0 then "ok"
    when  2 then "unable to get issuer certificate"
    when  3 then "unable to get CRL"
    when  7 then "certificate signature failure"
    when  8 then "CRL signature failure"
    when  9 then "certificate is not yet valid"
    when 10 then "certificate has expired"
    when 11 then "CRL is not yet valid"
    when 12 then "CRL has expired"
    when 18 then "self signed certificate"
    when 19 then "self signed certificate in chain"
    when 20 then "unable to get local issuer certificate"
    when 21 then "unable to verify leaf signature"
    when 23 then "certificate revoked"
    when 26 then "invalid purpose"
    when 27 then "certificate untrusted"
    when 28 then "certificate rejected"
    else         "verification error #{code}"
    end
  end
end
