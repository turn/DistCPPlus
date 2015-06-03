/**
 *
 */


import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.mapred.Reporter;

/**
 * Note this code is borrowed from com.turn.platform.cheetah.storage.parallel_etl.v2.ParallelETLMapReduceBase
 *
 * @author Yan Qi
 * @date May 14, 2012
 *
 * @Id $Id$
 * @Version $Rev$
 */
public class HadoopHeartBeat
{
	Timer timer = null;

	public void startHeartbeat(final Reporter reporter, long delay, long interval)
	{

		if (this.timer == null) {
			this.timer = new Timer(true);
			this.timer.schedule(new TimerTask() {
				@Override
				public void run() {
					reporter.progress();
				}
			}, delay, interval);
		}

	}

	public void stopHeatbeat()
	{
		if (this.timer != null) {
			this.timer.cancel();
			this.timer = null;
		}
	}

}
