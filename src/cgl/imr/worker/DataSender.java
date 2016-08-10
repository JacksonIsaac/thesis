package cgl.imr.worker;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class DataSender implements Runnable {

	private Socket sock ;
	private ConcurrentHashMap<String, DataHolder> dataCache;
	private DaemonWorker worker;

	public DataSender(Socket sock,	ConcurrentHashMap<String, DataHolder> dataCache, DaemonWorker worker) {
		this.dataCache = dataCache;
		this.sock = sock;	
		this.worker = worker;
	}

	public void run() {
		try {
			
			BufferedReader sockReader = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));
			String cmd = null;
			if((cmd = sockReader.readLine()) != null) {
				//System.out.println("Datasender cmd: " + cmd);
				if (cmd.equals("quit")) {
					worker.termintate();
					//this.daemonWorker.termintate();
					//break;
					System.exit(0);
				}else{
					DataOutputStream dout = new DataOutputStream(sock.getOutputStream());					
					DataHolder holder = dataCache.get(cmd);
					if (holder != null) {						
						byte[] data = holder.getData();

						//DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
						dout.write(data, 0, data.length);
						//System.out.println("Wrting data #################### "+data.length);
						dout.flush();
						dout.close();
						holder.decrementDownloadCount();
						if(holder.getDowloadCount()<=0)
						{
							//System.out.println("cache key removed: " + cmd);	
							//dataCache.put(cmd,null);
							dataCache.remove(cmd);
						}
				}
			}
			sockReader.close();						
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
