package org.apache.hadoop.security;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestMultiRealmUserAuthentication {

  @Test
  public void testReplaceRealmWithUserRealm(){

    Configuration conf = new Configuration();

    conf.set(MultiRealmUserAuthentication.KERBEROS_USER_REALM, "CORP.COM");

    String replaced = MultiRealmUserAuthentication.
    replaceRealmWithUserRealm("hadoop/hostname@HADOOP.COM", conf);
    assertEquals ("hadoop/hostname@CORP.COM",replaced  );
  }

  @Test
  public void testReplaceRealmWithUserRealmWithAUserPrincipal(){

    Configuration conf = new Configuration();

    conf.set(MultiRealmUserAuthentication.KERBEROS_USER_REALM, "CORP.COM");

    try {
      String replaced = MultiRealmUserAuthentication.
      replaceRealmWithUserRealm("hadoop/HADOOP.COM", conf);
      fail ();
    }
    catch (IllegalArgumentException e){
      //this is expected
    }
  }

}
