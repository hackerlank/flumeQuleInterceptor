package org.wxf.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.security.Timestamp;
import java.util.*;

import static org.wxf.flume.QuleLogInterceptor.Constants.*;

/**
 * Created by wangxufeng on 2014/10/21.
 */
public class QuleLogInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(QuleLogInterceptor.class);

    private String confPath = null;
    private boolean dynamicProp = false;

    //private String keywords = null;
    private String[] keywordsArray = null;

    private long propLastModify = 0L;
    private long propMonitorInterval;

    private boolean logWait = false;
    private long logWaitTime = 0L;
    private long logWaitCache = 0L;

    private long eventCount = 0l;
    private Queue<List> eventQueue = new LinkedList<List>();

    public QuleLogInterceptor(String conf_path, boolean dynamic_prop, long prop_monitor_interval, String keywords,
                              boolean log_wait, long log_wait_time, long log_wait_cache) {
        this.confPath = conf_path;
        this.dynamicProp = dynamic_prop;
        this.propMonitorInterval = prop_monitor_interval;
        //this.keywords = keywords;
        this.keywordsArray = keywords.split(",");
        this.logWait = log_wait;
        this.logWaitTime = log_wait_time;
        this.logWaitCache = log_wait_cache;
    }

    @Override
    public void initialize() {
        try {
            File confFile = new File(confPath);
            propLastModify = confFile.lastModified();
            FileInputStream fis = new FileInputStream(confFile);
            Properties props = new Properties();
            props.load(fis);

            //get wanted properties
            //String someProp = props.getProperty("property key");

        } catch (FileNotFoundException e) {
            logger.error("initializing - config file: " + confPath + " Not Found!");
        } catch (IOException e) {
            logger.error("initializing - config file: " + confPath + " cannot be read!");
        }
    }

    @Override
    public Event intercept(Event event) {
        ++eventCount;
        try {
            if (dynamicProp && eventCount > propMonitorInterval) {
                File confFile = new File(confPath);
                if (confFile.lastModified() > propLastModify) {
                    FileInputStream fis = new FileInputStream(confFile);
                    Properties props = new Properties();
                    props.load(fis);

                    //get wanted properties
                    //String someProp = props.getProperty("property key");
                }
            }

            //event headers
            //Map<String, String> headers = event.getHeaders();

            //event body bytes
            String body = new String(event.getBody());

            //let demanded row pass through
            for (String keyword: keywordsArray) {
                if (body.contains(keyword)) {

                    //execute log wait if set param to true
                    if (logWait) {
                        //push to queue
                        List<Map> eventList = new ArrayList<Map>();

                        Map<String, Long> timeHash = new HashMap<String, Long>();
                        Long cur_time = System.currentTimeMillis()/1000;
                        timeHash.put("time", cur_time);
                        eventList.add(timeHash);

                        Map<String, Event> eventHash = new HashMap<String, Event>();
                        eventHash.put("event", event);
                        eventList.add(eventHash);

                        eventQueue.offer(eventList);

                        eventList = eventQueue.element();
                        timeHash = eventList.get(0);
                        Long top_el_time = timeHash.get("time");
logger.debug(eventQueue.toString());
logger.debug("---------------------eventQueueSize: " + eventQueue.size());
logger.debug("---------------------waitTimeDiff: " + (cur_time - top_el_time));
                        if ((cur_time - top_el_time) > logWaitTime || eventQueue.size() > logWaitCache) {
                            eventHash = eventList.get(1);
                            Event top_el_event = eventHash.get("event");

                            //poll queue
                            eventQueue.poll();

                            return top_el_event;
                        }
                    } else {
                        return event;
                    }
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("intercepting - config file: " + confPath + " Not Found!");
        } catch (IOException e) {
            logger.error("intercepting - config file: " + confPath + " cannot be read!");
        } catch (Exception e) {
            logger.error("intercepting - caught unknown exceptions");
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> list = new LinkedList<Event>();
        for (Event event: events) {
            Event e = intercept(event);
            if (e != null) {
                list.add(e);
            }
        }
        return list;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder {
        private String confPath;
        private boolean dynamicProp;
        private long propMonitorInterval;
        private String keywords;
        private boolean logWait;
        private long logWaitTime;
        private long logWaitCache;

        @Override
        public Interceptor build() {
            return new QuleLogInterceptor(confPath, dynamicProp, propMonitorInterval, keywords, logWait, logWaitTime, logWaitCache);
        }

        @Override
        public void configure(Context context) {
            confPath = context.getString(CONF_PATH);
            dynamicProp = context.getBoolean(DYNAMIC_PROP, DYNAMIC_PROP_DFLT);
            propMonitorInterval = context.getLong(PROP_MONITOR_INTERVAL, PROP_MONITOR_INTERVAL_DFLT);
            keywords = context.getString(KEYWORDS);
            logWait = context.getBoolean(LOG_WAIT, LOG_WAIT_DFLT);
            logWaitTime = context.getLong(LOG_WAIT_TIME, LOG_WAIT_TIME_DFLT);
            logWaitCache = context.getLong(LOG_WAIT_CACHE, LOG_WAIT_CACHE_DFLT);
        }
    }

    public static class Constants {
        public static String CONF_PATH = "confpath";

        public static String DYNAMIC_PROP = "dynamicprop";
        public static boolean DYNAMIC_PROP_DFLT = false;

        public static String KEYWORDS = "keywords";

        public static String PROP_MONITOR_INTERVAL = "prop.monitor.rollInterval";
        public static long PROP_MONITOR_INTERVAL_DFLT = 500000L;

        public static String LOG_WAIT = "logwait";
        public static boolean LOG_WAIT_DFLT = false;
        public static String LOG_WAIT_TIME = "logwaittime";
        public static long LOG_WAIT_TIME_DFLT = 0L;                 //seconds
        public static String LOG_WAIT_CACHE = "logwaitcache";
        public static long LOG_WAIT_CACHE_DFLT = 0L;
    }
}
