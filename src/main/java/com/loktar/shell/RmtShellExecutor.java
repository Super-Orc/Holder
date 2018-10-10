//package com.loktar.shell;
//
//
//import ch.ethz.ssh2.ChannelCondition;
//import ch.ethz.ssh2.Connection;
//import ch.ethz.ssh2.Session;
//import ch.ethz.ssh2.StreamGobbler;
//import com.orco.spark.HolderConf;
//import org.apache.commons.io.IOUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.nio.charset.Charset;
//
///**
// * 远程执行shell脚本类
// *
// * @author l
// */
//public class RmtShellExecutor {
//	private static final Logger logger = LoggerFactory.getLogger(RmtShellExecutor.class);
//
//	private static RmtShellExecutor ins = new RmtShellExecutor();
//
//	private RmtShellExecutor() {
//	}
//
//	public static RmtShellExecutor getIns() {
//		return ins;
//	}
//
//	/**  */
//	private Connection conn;
//	/**
//	 * 远程机器IP
//	 */
//	private String ip;
//	/**
//	 * 用户名
//	 */
//	private String usr;
//	/**
//	 * 密码
//	 */
//	private String psword;
//	private String charset = Charset.defaultCharset().toString();
//
//	private static final int TIME_OUT = 1000 * 5 * 60;
//
////    /**
////     * 构造函数
////     * @param param 传入参数Bean 一些属性的getter setter 实现略
////     */
////    public RmtShellExecutor(ShellParam param) {
////        this.ip = param.getIp();
////        this.usr = param.getUsername();
////        this.psword = param.getPassword();
////    }
//
//	/**
//	 * 构造函数
//	 *
//	 * @param ip
//	 * @param usr
//	 * @param ps
//	 */
////	public RmtShellExecutor(String ip, String usr, String ps) {
////		this.ip = ip;
////		this.usr = usr;
////		this.psword = ps;
////	}
//
//	/**
//	 * 登录
//	 *
//	 * @return
//	 * @throws IOException
//	 */
//	private boolean login() throws IOException {
//		conn = new Connection(ip);
//		conn.connect();
//		return conn.authenticateWithPassword(usr, psword);
//	}
//
//	/**
//	 * 执行脚本
//	 *
//	 * @param cmds
//	 * @return
//	 * @throws Exception
//	 */
//	public int exec(String cmds) throws Exception {
//		InputStream stdOut = null;
//		InputStream stdErr = null;
//		String outStr;
//		String outErr;
//		int ret;
//		try {
//			if (login()) {
//				// Open a new {@link Session} on this connection
//				Session session = conn.openSession();
//				// Execute a command on the remote machine.
//				session.execCommand(cmds);
//
//				stdOut = new StreamGobbler(session.getStdout());
//				outStr = processStream(stdOut, charset);
//
//				stdErr = new StreamGobbler(session.getStderr());
//				outErr = processStream(stdErr, charset);
//
//				session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);
//
//				System.out.println("outStr=" + outStr);
//				System.out.println("outErr=" + outErr);
//
//				ret = session.getExitStatus();
//			} else {
//				throw new Exception("登录远程机器失败" + ip);
//			}
//		} finally {
//			if (conn != null) {
//				conn.close();
//			}
//			IOUtils.closeQuietly(stdOut);
//			IOUtils.closeQuietly(stdErr);
//		}
//		return ret;
//	}
//
//	/**
//	 * @param in
//	 * @param charset
//	 * @return
//	 * @throws IOException
//	 */
//	private String processStream(InputStream in, String charset) throws Exception {
//		byte[] buf = new byte[1024];
//		StringBuilder sb = new StringBuilder();
//		while (in.read(buf) != -1) {
//			sb.append(new String(buf, charset));
//		}
//		return sb.toString();
//	}
//
//	public void runShell(HolderConf conf,String ip) {
//		this.ip = ip;
//		//todo 这里还没写进 conf
//		this.usr = conf.get("spark.deploy.username", "charmer");
//		this.psword = conf.get("spark.deploy.password", "Rlhz!8758RC!@");
//
//		try {
//			exec("sh /home/charmer/zf/ha.sh");
//		} catch (Exception e) {
//			logger.error("runShell error!", e);
//			e.printStackTrace();
//		}
//	}
//
//	public static void main(String args[]) throws Exception {
////        RmtShellExecutor exe = new RmtShellExecutor("192.168.10.185", "orco", "admin");
////		RmtShellExecutor exe = new RmtShellExecutor("116.62.139.97", "charmer", "Rlhz!8758RC!@");
//		// 执行myTest.sh 参数为java Know dummy
////		System.out.println(exe.exec("sh /home/charmer/zf/ha.sh"));
////        System.out.println(exe.exec("sh /home/orco/a.sh"));
////        exe.exec("uname -a && date && uptime && who");
//	}
//}
////source /etc/profile
////        source /home/orco/.bash_profile
////        nohup java -Xmx1024m -Xms1024m -cp \
////        /home/orco/rc-mqtt-1.0-SNAPSHOT-jar-with-dependencies.jar \
////        com.collect.mqtt.MqttService_LD1012 >> /home/orco/pro.log 2>&1 &