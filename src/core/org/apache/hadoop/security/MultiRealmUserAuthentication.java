package org.apache.hadoop.security;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/** 
 * Utility class to support users and servers belonging two different realms
 *
 */

public class MultiRealmUserAuthentication {
  private static final Log LOG = LogFactory.getLog(MultiRealmUserAuthentication.class);
  //configuration property name for the user realm
  public static String  KERBEROS_USER_REALM ="hadoop.security.authentication.userrealm";

  // class variable used to store the Subject
  private  static UserGroupInformation  ugi ;

  /**
   * return the subject for server Principal  in the user realm
   * This will be the same name as the server principal of the default realm with the
   *  realm name replaced with the user realm name.
   *  Once created, the the UGI is cached.
   * @param conf
   * @return UserGroupInformation
   */
  public static UserGroupInformation getServerUGIForUserRealm (Configuration conf){
    if (ugi == null) {
      return getServerUGI (conf.get(KERBEROS_USER_REALM));
    }
    return ugi;
  }

  /**
   * returns true if this is a user in a different realm than the default 
   * realm of the Hadoop servers.
   * returns true if all the following conditions are satisfied
   * 	a) if there is a different user realm
   *  b) if the user is not a server
   *  c) if the user is part of the user realm
   * @param ticket
   * @param conf
   * @return boolean
   */
  public static  boolean isAUserInADifferentRealm(UserGroupInformation ticket,
      Configuration conf) {
    if (isEnabled (conf)){
      String fullName = ticket.getUserName();
      String names[] = SaslRpcServer.splitKerberosName(fullName);
      //make sure that it is not a server
      if (names.length < 3) {
        //check if the principal belongs to user realm
        if (fullName.toLowerCase().endsWith(conf.get(KERBEROS_USER_REALM).toLowerCase())){
          return true;
        }				
      }	    	 			 
    }
    return false;
  }
  
  /**
   * replaces the realm part of the principal name with the user realm
   * This method will be invoked by client side
   * @param principalName
   * @param conf
   * @return string value containing server principal in user realm
   */
  public static String replaceRealmWithUserRealm(
      String principalName, Configuration conf ) {
    return replaceRealm ( principalName, conf.get(KERBEROS_USER_REALM));
  }

  private static boolean isEnabled (Configuration conf){
    return (conf.get(KERBEROS_USER_REALM) != null);
  }

  private static synchronized  UserGroupInformation getServerUGI(String userRealm){
    UserGroupInformation current;
    try {
      current = UserGroupInformation.getCurrentUser();
      String principalName = current.getUserName();

      String principalInUserRealm = replaceRealm ( principalName, userRealm);
      ugi = UserGroupInformation.loginServerFromCurrentKeytabAndReturnUGI
                                               (principalInUserRealm);
      return ugi;
    } catch (IOException e) {
      LOG.warn("Current user information cannot be obtained", e);
      return null;
    }
  }

  private static String replaceRealm(String principalName, String userRealm) {
    String[] parts = principalName.split("[/@]");

    if (parts.length >2){
      String[] serverParts = parts[1].split("[.]");
      String serverName  = serverParts[0] + "." + userRealm.toLowerCase();
      return parts[0] + "/" +  serverName + "@" + userRealm;
    }
    else {
      LOG.warn ("The serverPrincipal = " + principalName +
      "doesn't confirm to the standards");
      throw new IllegalArgumentException();
    }
  }
}
