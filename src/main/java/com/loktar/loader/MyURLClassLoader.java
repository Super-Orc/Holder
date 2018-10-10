//package com.loktar.loader;
//
//
//import com.orco.spark.util.ThreadUtils;
//import org.apache.hadoop.conf.Configuration;
//
//import java.lang.reflect.Method;
//import java.util.concurrent.ScheduledExecutorService;
//
//public class MyURLClassLoader {
//
//	public static void main(String[] args) {
//
//		ScheduledExecutorService service = ThreadUtils.newDaemonSingleThreadScheduledExecutor("quartz-thread");
//		for (int i = 0; i < 10; i++) {
//			Thread s = new Thread();
//			service.submit(new Runnable() {
//				@Override
//				public void run() {
//					DynamicClassLoader CLASS_LOADER = new DynamicClassLoader(new Configuration(), s.getContextClassLoader());
//					Class<?> aClass = null;
//					try {
//						aClass = Class.forName("com.com.dynjar.TestDynJar", true, CLASS_LOADER);
//						Method doSth = aClass.getDeclaredMethod("doSth");
//						doSth.invoke(aClass.newInstance());
//						Thread.sleep(100000);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				}
//			});
//			try {
//				s.stop();
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
//
//}