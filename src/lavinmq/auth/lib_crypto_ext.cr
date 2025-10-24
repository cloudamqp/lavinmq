# src/lavinmq/auth/lib_crypto_ext.cr
lib LibCrypto
  # Types
  alias BIGNUM = Void*
  type RSA = Void*
  type EVP_PKEY = Void*
  type EVP_PKEY_CTX = Void*

  # BIGNUM functions
  fun bn_bin2bn = BN_bin2bn(s : UInt8*, len : Int32, ret : BIGNUM) : BIGNUM

  # RSA functions
  fun rsa_new = RSA_new : RSA
  fun rsa_free = RSA_free(rsa : RSA)
  fun rsa_set0_key = RSA_set0_key(rsa : RSA, n : BIGNUM, e : BIGNUM, d : BIGNUM) : Int32
  fun pem_write_bio_rsa_pubkey = PEM_write_bio_RSA_PUBKEY(bio : Bio*, rsa : RSA) : Int32

  # EVP (high-level crypto) functions
  fun evp_pkey_free = EVP_PKEY_free(pkey : EVP_PKEY)
  fun pem_read_bio_pubkey = PEM_read_bio_PUBKEY(bio : Bio*, x : EVP_PKEY*, cb : Void*, u : Void*) : EVP_PKEY
  fun evp_digestverify_init = EVP_DigestVerifyInit(ctx : EVP_MD_CTX, pctx : EVP_PKEY_CTX*, type : EVP_MD, e : Void*, pkey : EVP_PKEY) : Int32
  fun evp_digestverify = EVP_DigestVerify(ctx : EVP_MD_CTX, sig : UInt8*, siglen : LibC::SizeT, tbs : UInt8*, tbslen : LibC::SizeT) : Int32
  fun evp_md_ctx_new = EVP_MD_CTX_new : EVP_MD_CTX
  fun evp_md_ctx_free = EVP_MD_CTX_free(ctx : EVP_MD_CTX)

  # BIO functions (shared)
  fun bio_s_mem = BIO_s_mem : BioMethod*
  fun bio_write = BIO_write(bio : Bio*, data : UInt8*, len : Int32) : Int32
  fun bio_read = BIO_read(bio : Bio*, data : UInt8*, len : Int32) : Int32
  fun bio_ctrl = BIO_ctrl(bio : Bio*, cmd : Int32, larg : LibC::Long, parg : Void*) : LibC::Long
end
