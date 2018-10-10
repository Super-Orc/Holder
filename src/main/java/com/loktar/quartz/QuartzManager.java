//package com.loktar.quartz;
//
//
//import com.loktar.bean.TxQuartz;
//import com.loktar.util.CollectionUtil;
//import com.loktar.util.Consts;
//import com.orco.spark.SparkException;
//import org.quartz.*;
//import org.quartz.impl.StdSchedulerFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.net.URLClassLoader;
//import java.sql.SQLException;
//import java.util.*;
//import java.util.stream.Collectors;
//
//public class QuartzManager {
//
//	private static final Logger logger = LoggerFactory.getLogger(QuartzManager.class);
//
//	private static SchedulerFactory schedulerFactory = new StdSchedulerFactory();
//
//	static void  onStart(ArrayList<Object> objs){
//		try {
//			List<TxQuartz> quartzs = new PostgresClient(null).getTxQuartzs();
//			Map<String, TxQuartz> names = quartzs.stream().collect(Collectors.toMap(TxQuartz::getName, a -> a));
//
//			Collection<String> diff = CollectionUtil.getDiffent(names.keySet(), Consts.RUNNING_JOB.keySet());
//			diff.forEach(name -> {
//				try {
//					if (Consts.RUNNING_JOB.containsKey(name) && !names.containsKey(name)) {
//						QuartzManager.removeJob(Consts.RUNNING_JOB.get(name));
//					} else if (!Consts.RUNNING_JOB.containsKey(name) && names.containsKey(name)) {
//						if (names.get(name).getType() == Consts.EXTERNAL) {
//							Class<?> clazz = getClazz(names.get(name).getPath(), names.get(name).getMain());
//							if (clazz != null)
//								QuartzManager.addJob(clazz, names.get(name));
//						} else {
//							QuartzManager.addJob(Class.forName(names.get(name).getMain()), names.get(name));
//						}
//						logger.info(Arrays.toString(Consts.RUNNING_JOB.keySet().toArray()));
//					}
//				} catch (Exception ee) {
//					ee.printStackTrace();
//				}
//			});
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
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
//	static void addJob(Class jobClass, TxQuartz info) throws SparkException {
//		try {
//			Scheduler scheduler = schedulerFactory.getScheduler();
//			JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(info.getName()).build();
//
//			String cronExpression = info.getCron().trim();
//
//			CronTrigger trigger = TriggerBuilder.newTrigger()
//					.withIdentity(info.getName())
//					.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
//					.build();
//
//			scheduler.scheduleJob(jobDetail, trigger);
//
//			if (!scheduler.isShutdown()) {
//				scheduler.start();
//			}
//			Consts.RUNNING_JOB.put(info.getName(), info);
//		} catch (Exception e) {
//			logger.error("addJob error",e);
//			throw new SparkException("addJob error."+ info.toString());
//		}
//	}
//
//	/**
//	 * 修改一个任务的触发时间
//	 *
//	 * @param triggerName      触发器名
//	 * @param triggerGroupName 触发器组名
//	 * @param cron             时间设置，参考quartz说明文档
//	 */
//	public static void modifyJobTime(String jobName,
//									 String jobGroupName, String triggerName, String triggerGroupName, String cron) {
//		try {
//			Scheduler sched = schedulerFactory.getScheduler();
//			TriggerKey triggerKey = TriggerKey.triggerKey(triggerName, triggerGroupName);
//			CronTrigger trigger = (CronTrigger) sched.getTrigger(triggerKey);
//			if (trigger == null) {
//				return;
//			}
//
//			String oldTime = trigger.getCronExpression();
//			if (!oldTime.equalsIgnoreCase(cron)) {
//				/** 方式一 ：调用 rescheduleJob 开始 */
//				// 触发器
//				TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.newTrigger();
//				// 触发器名,触发器组
//				triggerBuilder.withIdentity(triggerName, triggerGroupName);
//				triggerBuilder.startNow();
//				// 触发器时间设定
//				triggerBuilder.withSchedule(CronScheduleBuilder.cronSchedule(cron));
//				// 创建Trigger对象
//				trigger = (CronTrigger) triggerBuilder.build();
//				// 方式一 ：修改一个任务的触发时间
//				sched.rescheduleJob(triggerKey, trigger);
//				/** 方式一 ：调用 rescheduleJob 结束 */
//
//				/** 方式二：先删除，然后在创建一个新的Job  */
//				//JobDetail jobDetail = sched.getJobDetail(JobKey.jobKey(jobName, jobGroupName));
//				//Class<? extends Job> jobClass = jobDetail.getJobClass();
//				//removeJob(jobName, jobGroupName, triggerName, triggerGroupName);
//				//addJob(jobName, jobGroupName, triggerName, triggerGroupName, jobClass, cron);
//				/** 方式二 ：先删除，然后在创建一个新的Job */
//			}
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	/**
//	 * 移除一个任务
//	 */
//	public static void removeJob(TxQuartz info) {
//		try {
//			Scheduler sched = schedulerFactory.getScheduler();
//
//			TriggerKey triggerKey = TriggerKey.triggerKey(info.getName());
//
//			sched.pauseTrigger(triggerKey);// 停止触发器
//			sched.unscheduleJob(triggerKey);// 移除触发器
//			sched.deleteJob(JobKey.jobKey(info.getName()));// 删除任务
//			Consts.RUNNING_JOB.remove(info.getName());
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	/**
//	 * 启动所有定时任务
//	 */
//	public static void startJobs() {
//		try {
//			Scheduler sched = schedulerFactory.getScheduler();
//			sched.start();
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	/**
//	 * 关闭所有定时任务
//	 */
//	public static void shutdownJobs() {
//		try {
//			Scheduler sched = schedulerFactory.getScheduler();
//			if (!sched.isShutdown()) {
//				sched.shutdown();
//			}
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//}