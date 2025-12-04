# Add missing LibCrypto function bindings
lib LibCrypto
  alias X509CRL = Void*
  alias DistPoint = Void*
  alias DistPoints = Void*
  alias GeneralNamesPtr = Void*
  alias GeneralNamePtr = Void*
  alias ASN1String = Void*
  alias ASN1Time = Void*

  # DIST_POINT structure
  struct DistPointStruct
    distpoint : DistPointNameStruct* # DIST_POINT_NAME
    reasons : Void*                  # ASN1_BIT_STRING (different from ASN1_STRING)
    crl_issuer : GeneralNamesPtr     # GENERAL_NAMES
    dp_reasons : LibC::Int
  end

  # DIST_POINT_NAME structure (union-like)
  struct DistPointNameStruct
    type : LibC::Int
    name : DistPointNameUnion
  end

  # Union for DIST_POINT_NAME
  union DistPointNameUnion
    fullname : GeneralNamesPtr # GENERAL_NAMES (type 0)
    relativename : Void*       # X509_NAME (type 1)
  end

  # GENERAL_NAME structure (simplified)
  struct GeneralNameStruct
    type : LibC::Int
    d : GeneralNameData
  end

  # Union for GENERAL_NAME data
  union GeneralNameData
    ptr : LibC::Char*
    otherName : Void*                      # ameba:disable Naming/VariableNames
    rfc822Name : ASN1String                # ameba:disable Naming/VariableNames
    dNSName : ASN1String                   # ameba:disable Naming/VariableNames
    x400Address : Void*                    # ameba:disable Naming/VariableNames
    directoryName : Void*                  # ameba:disable Naming/VariableNames
    ediPartyName : Void*                   # ameba:disable Naming/VariableNames
    uniformResourceIdentifier : ASN1String # ameba:disable Naming/VariableNames
    iPAddress : ASN1String                 # ameba:disable Naming/VariableNames
    registeredID : Void*                   # ameba:disable Naming/VariableNames
  end

  # Create a BIO from file
  fun bio_new_file = BIO_new_file(filename : LibC::Char*, mode : LibC::Char*) : Bio*

  # Free a BIO
  fun bio_free = BIO_free(bio : Bio*) : LibC::Int

  # Read certificate from PEM
  fun pem_read_bio_x509 = PEM_read_bio_X509(
    bp : Bio*,
    x : X509*,
    cb : Void*,
    u : Void*,
  ) : X509

  # Read a CRL from a PEM file
  fun pem_read_bio_x509_crl = PEM_read_bio_X509_CRL(
    bp : Bio*,
    x : X509CRL*,
    cb : Void*,
    u : Void*,
  ) : X509CRL

  # Get extension from certificate by NID
  fun x509_get_ext_d2i = X509_get_ext_d2i(
    x : X509,
    nid : LibC::Int,
    crit : LibC::Int*,
    idx : LibC::Int*,
  ) : Void*

  # Free X509 certificate
  fun x509_free = X509_free(a : X509)

  # Generic OpenSSL stack functions (work with any stack type via Void*)
  # Get number of elements in an OpenSSL stack
  fun openssl_sk_num = OPENSSL_sk_num(sk : Void*) : LibC::Int

  # Get element from OpenSSL stack by index
  fun openssl_sk_value = OPENSSL_sk_value(sk : Void*, idx : LibC::Int) : Void*

  # Free CRL Distribution Points
  fun crl_dist_points_free = CRL_DIST_POINTS_free(a : DistPoints)

  # Add a CRL to the X509_STORE
  fun x509_store_add_crl = X509_STORE_add_crl(ctx : X509_STORE, x : X509CRL) : LibC::Int

  # Free a CRL
  fun x509_crl_free = X509_CRL_free(a : X509CRL)

  # Get revoked certificates from CRL
  fun x509_crl_get_revoked = X509_CRL_get_REVOKED(crl : X509CRL) : Void*

  # Set X509 verification flags on the store
  fun x509_store_set_flags = X509_STORE_set_flags(ctx : X509_STORE, flags : X509VerifyFlags) : LibC::Int

  # Create a new X509_STORE
  fun x509_store_new = X509_STORE_new : X509_STORE

  # Free an X509_STORE (decrements reference count)
  fun x509_store_free = X509_STORE_free(store : X509_STORE)

  # Convert ASN1_STRING to UTF8
  fun asn1_string_to_utf8 = ASN1_STRING_to_UTF8(out : LibC::Char**, in : ASN1String) : LibC::Int

  # Free memory allocated by OpenSSL (OPENSSL_free is a macro for CRYPTO_free)
  fun openssl_free = CRYPTO_free(addr : Void*, file : LibC::Char*, line : LibC::Int)

  # Get the nextUpdate time from a CRL
  fun x509_crl_get_next_update = X509_CRL_get_nextUpdate(crl : X509CRL) : ASN1Time

  # Convert ASN1_TIME to time_t (Unix timestamp)
  fun asn1_time_to_tm = ASN1_TIME_to_tm(s : ASN1Time, tm : LibC::Tm*) : LibC::Int
end
