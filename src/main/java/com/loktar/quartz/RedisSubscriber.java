//package com.loktar.quartz;
//
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import redis.clients.jedis.JedisPubSub;
//
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.net.URLClassLoader;
//
///**
// * 1、要读哪个工程的哪个表，这个用 redis 传过来
// * 2、然后在 onMessage 里开始读表，并运行 Quartz 任务
// */
//@Deprecated
//public class RedisSubscriber extends JedisPubSub {
//
//	private static final Logger logger = LoggerFactory.getLogger(RedisSubscriber.class);
//
////	private static RedisSubscriber ins = new RedisSubscriber();
////
////	public static RedisSubscriber getIns() {
////		return ins;
////	}
////
////	private RedisSubscriber() {
////	}
////	public void onMessage(String channel, String message) {
////		logger.info("redis subscriber start, onMessage working.");
////		if (channel.equals("loadTable")) {
////			Db db = JSON.parseObject(message, Db.class);
////			try {
////				List<TxQuartz> quartzs = new PostgresClient(db).getTxQuartzs();
////				Map<String, TxQuartz> names = quartzs.stream().collect(Collectors.toMap(TxQuartz::getName, a -> a));
////
////				Collection<String> diff = CollectionUtil.getDiffent(names.keySet(), Consts.RUNNING_JOB.keySet());
////				diff.forEach(name -> {
////					try {
////						if (Consts.RUNNING_JOB.containsKey(name) && !names.containsKey(name)) {
////							QuartzManager.removeJob(Consts.RUNNING_JOB.get(name));
////						} else if (!Consts.RUNNING_JOB.containsKey(name) && names.containsKey(name)) {
////							if (names.get(name).getType() == Consts.EXTERNAL) {
////								Class<?> clazz = getClazz(names.get(name).getPath(), names.get(name).getMain());
////								if (clazz != null)
////									QuartzManager.addJob(clazz, names.get(name));
////							} else {
////								QuartzManager.addJob(Class.forName(names.get(name).getMain()), names.get(name));
////							}
////							logger.info(Arrays.toString(Consts.RUNNING_JOB.keySet().toArray()));
////						}
////					} catch (Exception ee) {
////						ee.printStackTrace();
////					}
////				});
////			} catch (SQLException e) {
////				e.printStackTrace();
////			}
////		}
////	}
//
//	private static Class<?> getClazz(String path, String main) {
//		Class<?> clazz = null;
//		try {
//			URL url = new URL(path);
//			URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{url}, Thread.currentThread().getContextClassLoader());
//			clazz = urlClassLoader.loadClass(main);
//		} catch (MalformedURLException | ClassNotFoundException e) {
//			e.printStackTrace();
//		}
//		return clazz;
//	}
//
////	public void onStart() {
////		try {
////			int a = 1 / 0;
////			throw new SparkException("11");
////		} catch (SparkException e1) {
////			e1.printStackTrace();
////		}
////		ExecutorService pool = Executors.newSingleThreadExecutor();
////		pool.execute(() -> {
////			try {
////				Jedis jedis = RedisUtil.pool().getResource();
////				logger.info("redis start subscribe.");
////				jedis.subscribe(new RedisSubscriber(), "loadTable");
////			} catch (Exception e) {
////				logger.error("redis subscribe error", e);
////
////			}
////		});
////	}
//}
