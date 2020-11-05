package com.macfred.server;

import com.macfred.server.util.InternetUtil;

public class PortAllocator {
	private int mRangeStart;
	private int mRangeEnd;

	PortAllocator(int rangeStart, int rangeEnd) {
		mRangeStart = rangeStart;
		mRangeEnd = rangeEnd;
	}

	public int allocate() {
		for (int candidatePort = mRangeStart; candidatePort <= mRangeEnd; candidatePort++) {
			if (InternetUtil.available(candidatePort)) {
				return candidatePort;
			}
		}
		throw new RuntimeException("Failed to allocate port");
	}
}
