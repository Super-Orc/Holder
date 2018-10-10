package com.orco.holder.deploy


import com.orco.holder.deploy.master.MasterStandByInfo
import com.orco.holder.executor.ContainerInfo
import com.orco.holder.rpc.{RpcAddress, RpcEndpointRef}
import com.orco.holder.util.Utils

private[deploy] sealed trait DeployMessage extends Serializable

/** Contains messages sent between Scheduler endpoint nodes. */
private[deploy] object DeployMessages {

  case object SendHeartbeat

  case class RegisterMasterSb(
                               id: String,
                               host: String,
                               port: Int,
                               masterSb: RpcEndpointRef,
                               standByWebUiUrl: String,
                               masterAddress: RpcAddress)
    extends DeployMessage {
    Utils.checkHost(host)
    assert(port > 0)
  }

  case class Heartbeat(masterId: String, master: RpcEndpointRef) extends DeployMessage


  sealed trait RegisterMasterSbResponse


  case class RegisteredMasterSb(standByInfo: MasterStandByInfo,
                                masterAddress: RpcAddress) extends DeployMessage with RegisterMasterSbResponse

  //  case class RegisteredMasterSb(
  //                                 master: RpcEndpointRef,
  //                                 masterAddress: RpcAddress) extends DeployMessage with RegisterMasterSbResponse

  case class RegisterMasterSbFailed(message: String) extends DeployMessage with RegisterMasterSbResponse

  case class ReregisterToMaster(master: Option[RpcEndpointRef]) // used when a masterStandby attempts to reconnect to a master

  case class MasterChanged(master: RpcEndpointRef)

  //  case class RegisterWorker(
  //                             id: String,
  //                             host: String,
  //                             port: Int,
  //                             worker: RpcEndpointRef,
  //                             cores: Int,
  //                             memory: Int,
  //                             workerWebUiUrl: String,
  //                             masterAddress: RpcAddress)
  //    extends DeployMessage {
  //    Utils.checkHost(host)
  //    assert (port > 0)
  //  }


  //
  //  case class DriverStateChanged(
  //                                 driverId: String,
  //                                 state: DriverState,
  //                                 exception: Option[Exception])
  //    extends DeployMessage
  //
  //  case class WorkerSchedulerStateResponse(id: String, executors: List[ExecutorDescription],
  //                                          driverIds: Seq[String])
  //
  //  /**
  //    * A worker will send this message to the master when it registers with the master. Then the
  //    * master will compare them with the executors and drivers in the master and tell the worker to
  //    * kill the unknown executors and drivers.
  //    */
  //  case class WorkerLatestState(
  //                                id: String,
  //                                executors: Seq[ExecutorDescription],
  //                                driverIds: Seq[String]) extends DeployMessage

  //  case class Heartbeat(workerId: String, worker: RpcEndpointRef) extends DeployMessage


  //  sealed trait RegisterWorkerResponse


  //  case class RegisteredWorker(
  //                               master: RpcEndpointRef,
  //                               masterWebUiUrl: String,
  //                               masterAddress: RpcAddress) extends DeployMessage with RegisterWorkerResponse
  //
  //  case class RegisterWorkerFailed(message: String) extends DeployMessage with RegisterWorkerResponse

  //  case object MasterInStandby extends DeployMessage with RegisterWorkerResponse
  //
  //  case class ReconnectWorker(masterUrl: String) extends DeployMessage
  //  case class ReconnectToMaster(masterUrl: String) extends DeployMessage
  //
  //  case class KillExecutor(masterUrl: String, appId: String, execId: Int) extends DeployMessage
  //
  //  case class LaunchExecutor(
  //                             masterUrl: String,
  //                             appId: String,
  //                             execId: Int,
  //                             appDesc: ApplicationDescription,
  //                             cores: Int,
  //                             memory: Int)
  //    extends DeployMessage
  //
  //  case class LaunchDriver(driverId: String, driverDesc: DriverDescription) extends DeployMessage
  //
  //  case class KillDriver(driverId: String) extends DeployMessage
  //
  //  case class ApplicationFinished(id: String)
  //
  //  // Worker internal
  //
  //  case object WorkDirCleanup // Sent to Worker endpoint periodically for cleaning up app folders

  //  case object ReregisterWithMaster // used when a worker attempts to reconnect to a master


  // AppClient to Master

  //  case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef)
  //    extends DeployMessage
  //
  //  case class UnregisterApplication(appId: String)
  //
  //  case class MasterChangeAcknowledged(appId: String)
  //
  //  case class RequestExecutors(appId: String, requestedTotal: Int)
  //
  //  case class KillExecutors(appId: String, executorIds: Seq[String])
  //
  //  // Master to AppClient
  //
  //  case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends DeployMessage
  //
  //  case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
  //    Utils.checkHostPort(hostPort)
  //  }
  //
  //  case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String],
  //                             exitStatus: Option[Int], workerLost: Boolean)
  //
  //  case class ApplicationRemoved(message: String)
  //
  //  case class WorkerRemoved(id: String, host: String, message: String)
  //
  //  // DriverClient <-> Master
  //
  //  case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage
  //
  //  case class SubmitDriverResponse(
  //                                   master: RpcEndpointRef, success: Boolean, driverId: Option[String], message: String)
  //    extends DeployMessage
  //
  //  case class RequestKillDriver(driverId: String) extends DeployMessage
  //
  //  case class KillDriverResponse(
  //                                 master: RpcEndpointRef, driverId: String, success: Boolean, message: String)
  //    extends DeployMessage
  //
  //  case class RequestDriverStatus(driverId: String) extends DeployMessage
  //
  //  case class DriverStatusResponse(found: Boolean, state: Option[DriverState],
  //                                  workerId: Option[String], workerHostPort: Option[String], exception: Option[Exception])
  //
  //  // Internal message in AppClient
  //
  //  case object StopAppClient

  // Master to Worker & AppClient


  // MasterWebUI To Master

  case object MasterExistRequest

  case object RequestMasterState

  // Master to MasterWebUI

  case class MasterStateResponse(
                                  host: String,
                                  port: Int,
                                  //                                  restPort: Option[Int],
                                  isMaster: Boolean,
                                  masterWebUI: String,
                                  standBys: Array[MasterStandByInfo],
                                  activeApps: List[ContainerInfo],
                                  completeApps: List[ContainerInfo]) {

    Utils.checkHost(host)
    assert(port > 0)

    def uri: String = "holder://" + host + ":" + port

    //    def restUri: Option[String] = restPort.map { p => "holder://" + host + ":" + p }
  }

  //  WorkerWebUI to Worker

  //  case object RequestWorkerState

  // Worker to WorkerWebUI

  //  case class WorkerStateResponse(host: String, port: Int, workerId: String,
  //                                 executors: List[ExecutorRunner], finishedExecutors: List[ExecutorRunner],
  //                                 drivers: List[DriverRunner], finishedDrivers: List[DriverRunner], masterUrl: String,
  //                                 cores: Int, memory: Int, coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String) {
  //
  //    Utils.checkHost(host)
  //    assert (port > 0)
  //  }

  // Liveness checks in various places

  case object MasterWebUIRequest

  case class MasterWebUIResponse(masterWebUI: String)

//  case object ContainerSizeRequest
//
//  case class ContainerSizeResponse(size: Int)

  case class ContainerRunningRequest(message:String)
//  case class ContainerRunningRequest(info:ContainerInfo)

  case class ContainerRunningResponse(boolean: Boolean)


  case class NameContainerInfo(id: String, name: String, info: ContainerInfo)

  case class NewContainerRequest(info:ContainerInfo)
}
