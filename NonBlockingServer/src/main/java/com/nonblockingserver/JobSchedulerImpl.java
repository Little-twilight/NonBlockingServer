package com.nonblockingserver;

import com.zhongyou.jobschedule.JobSchedule;
import com.zhongyou.jobschedule.JobScheduler;
import com.zhongyou.util.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class JobSchedulerImpl extends JobScheduler {
	private ArrayList<JobSchedule> mJobSchedules = new ArrayList<>();
	private Queue<JobSchedule> mCheckJobs = new ArrayDeque<>();
	private Lock mJobScheduleLock = new ReentrantLock();
	private Condition mJobScheduleCondition = mJobScheduleLock.newCondition();

	void checkJobSchedule() throws InterruptedException {
		mCheckJobs.clear();
		long thermal = 20L;
		try {
			mJobScheduleLock.lock();
			long now = System.currentTimeMillis();
			while (!mJobSchedules.isEmpty()) {
				JobSchedule peek = mJobSchedules.get(0);
				if (peek.isCanceled()) {
					mJobSchedules.remove(0);
					continue;
				}
				if (mCheckJobs.isEmpty()
						|| peek.getSchedule() - mCheckJobs.peek().getSchedule() < thermal
						|| peek.getSchedule() - now < thermal
				) {
					mCheckJobs.offer(peek);
					mJobSchedules.remove(0);
				} else {
					break;
				}
			}
			if (mCheckJobs.isEmpty()) {
				mJobScheduleCondition.await();
			} else {
				long wait = mCheckJobs.peek().getSchedule() - now;
				if (mCheckJobs.peek().getSchedule() > 0 && wait > thermal) {
					if (mJobScheduleCondition.await(wait, TimeUnit.MILLISECONDS)) {
						mJobSchedules.addAll(mCheckJobs);
						Collections.sort(mJobSchedules);
						mCheckJobs.clear();
						return;
					}
				}
			}
		} finally {
			mJobScheduleLock.unlock();
		}
		for (JobSchedule job : mCheckJobs) {
			if (job.isCanceled()) {
				continue;
			}
			try {
				job.run();
			} catch (Exception e) {
				Logger.printException(e);
			}
		}
	}

	@Override
	protected void insertSchedule(JobSchedule schedule) {
		try {
			mJobScheduleLock.lock();
			mJobSchedules.add(schedule);
			for (int i = mJobSchedules.size() - 1; i >= 0; i--) {
				if (mJobSchedules.get(i).isCanceled()) {
					mJobSchedules.remove(i);
				}
			}
			Collections.sort(mJobSchedules);
			mJobScheduleCondition.signalAll();
		} finally {
			mJobScheduleLock.unlock();
		}
	}

}
