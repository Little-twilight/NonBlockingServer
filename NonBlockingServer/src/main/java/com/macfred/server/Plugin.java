package com.macfred.server;

import java.net.InetAddress;

public interface Plugin {

	void onInstall(NonBlockingServer server);

	void onUninstall();

	void onStart();

	void onStop();

	boolean isStarted();

	void onLocalAddressUpdated(InetAddress address);

}
