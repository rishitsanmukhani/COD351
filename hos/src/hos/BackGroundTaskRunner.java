package hos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class BackGroundTaskRunner implements ServletContextListener {
//	    private  ExecutorService executor;


	    private ScheduledExecutorService scheduler;

	    
	    @Override
	    public void contextInitialized(ServletContextEvent event) {
		    System.out.println(" starting background task runner");
		    scheduler = Executors.newSingleThreadScheduledExecutor();	
		    scheduler.scheduleAtFixedRate(new BackGroundTasks(BackGroundTasks.ARCHIVE_TASK), 30, 90,  TimeUnit.SECONDS);
		    scheduler.scheduleAtFixedRate(new BackGroundTasks(BackGroundTasks.UNARCHIVE_TASK), 60, 90,  TimeUnit.SECONDS);
		    scheduler.scheduleAtFixedRate(new BackGroundTasks(BackGroundTasks.CLEANUP_TASK), 90, 90,  TimeUnit.SECONDS);
	    }

	    @Override
	    public void contextDestroyed(ServletContextEvent event) {
	        scheduler.shutdownNow();
	    }
	    
}