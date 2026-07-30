# Bootstrap the first user as Super Administrator

An empty Agent Hub makes the first Hub User successfully created through enabled Password Registration, LDAP Login, or trusted external identity provisioning its initial Super Administrator, using an atomic first-user decision. Creating that user automatically disables public Password Registration, though an Administrator may deliberately reopen it later. Agent Hub requires no deployment token or separate setup ceremony, accepting first-arrival takeover risk in exchange for direct zero-configuration registration and login.
