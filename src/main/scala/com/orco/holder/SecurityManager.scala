package com.orco.holder

import com.orco.holder.internal.Logging
import com.orco.holder.network.sasl.SecretKeyHolder

private[holder] class SecurityManager(
                                      sparkConf: HolderConf,
                                      val ioEncryptionKey: Option[Array[Byte]] = None)
  extends Logging with SecretKeyHolder {

  // TODO: 注掉了
  private val secretKey = ""
//  private val secretKey = generateSecretKey()


  /**
    * Gets the user used for authenticating SASL connections.
    * For now use a single hardcoded user.
    * @return the SASL user as a String
    */
  def getSaslUser(): String = "sparkSaslUser"

  /**
    * Gets the secret key.
    * @return the secret key as a String if authentication is enabled, otherwise returns null
    */
  def getSecretKey(): String = secretKey

  // Default SecurityManager only has a single secret key, so ignore appId.
  override def getSaslUser(appId: String): String = getSaslUser()
  override def getSecretKey(appId: String): String = getSecretKey()


}
