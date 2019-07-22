package com.nonblockingserver;

public interface Plugin {

	void onInstall(NonBlockingServer server);

	void onUninstall();

	void onStart();

	void onStop();

	boolean isStarted();

}
