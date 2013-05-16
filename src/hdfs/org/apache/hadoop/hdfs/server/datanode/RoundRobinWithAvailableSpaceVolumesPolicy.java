package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolumeSet;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

public class RoundRobinWithAvailableSpaceVolumesPolicy implements
		BlockVolumeChoosingPolicy {
	private static final Log LOG = LogFactory.getLog(RoundRobinWithAvailableSpaceVolumesPolicy.class);
	
	private FSVolumeSet volumes;
	private int curVolume = 0;
	private int[] probability = null;
	private int max = 0;
	private Random random = new Random();
	private Thread refreshThread;
	private long refreshInterval;
	
	@Override
	public FSVolume chooseVolume(FSVolume[] volumes, long blockSize)
			throws IOException {
		if (volumes.length < 1) {
			throw new DiskOutOfSpaceException("No more available volumes");
		}
		// since volumes could've been removed because of the failure
		// make sure we are not out of bounds
		if (curVolume >= volumes.length) {
			curVolume = 0;
		}
		
		int startVolume = curVolume;
		while (true) {
			int idx = curVolume;
			FSVolume volume = volumes[idx];
			curVolume = (curVolume + 1) % volumes.length;
			if (volume.getAvailable() > blockSize) {
				synchronized (probability) {
					if (probability[idx] > random.nextInt(max)) {
						return volume;
					}
				}
			}
			if (curVolume == startVolume) {
				throw new DiskOutOfSpaceException(
						"Insufficient space for an additional block");
			}
		}
	}
	
	@Override
	public void initialize(FSVolumeSet _volumes, Configuration conf) throws IOException {
		volumes = _volumes;
		refreshInterval = conf.getLong("dfs.RoundRobinWithAvailableSpaceVolumesPolicy.interval", 600000L);
		probability = new int[volumes.getVolumes().length];	
		update();
		refreshThread = new Thread(new ProbabilityRefreshThread(), "ProbabilityRefreshThread");
		refreshThread.setDaemon(true);
		refreshThread.start();
	}

	private void update() throws IOException {
		FSVolume[] vols = volumes.getVolumes();
		synchronized (probability) {
			max=0;
			for (int idx = 0; idx < vols.length; idx++) {
				probability[idx] = (int) (vols[idx].getAvailable() / 1024 / 1024 / 1024);
				LOG.info("probability[" + idx +"]: "+ probability[idx]);
				if (probability[idx] > max) {
					max = probability[idx];
				}
			}
			LOG.info("max: "+ max);
		}
	}
	
	class ProbabilityRefreshThread implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(refreshInterval);
					try {
						update();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (InterruptedException e) {
				}
			}
		}
	}

}
