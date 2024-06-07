import CloudKit
import CloudKitCodable
import Combine
import Foundation
import os.log

public final class SyncEngine<Model: CloudKitCodable> {
  
  /// Represents the model changes
  public struct ModelChanges {
    public var updates: ModelChange?
    public var deletes: ModelChange?
    
    /// Updates that the server does not recognize. Caller may decide to delete these locally or re-add by clearing out cloudkit meta data
    public var unknownUpdates: ModelChange?
    /// Items that the server no longer contains
    public var unknownDeletes: ModelChange?
    
    /// Change token associated with these changes. The caller must call SyncEngine
    /// This is only required when changeToken is not `nil`
    public var changeToken: CKServerChangeToken?
    
    public init(updates: SyncEngine<Model>.ModelChange? = nil, 
                deletes: SyncEngine<Model>.ModelChange? = nil,
                unknownUpdates: SyncEngine<Model>.ModelChange? = nil,
                unknownDeletes: SyncEngine<Model>.ModelChange? = nil,
                changeToken: CKServerChangeToken? = nil) {
      self.updates = updates
      self.deletes = deletes
      self.unknownUpdates = unknownUpdates
      self.unknownDeletes = unknownDeletes
      self.changeToken = changeToken
    }
  }
  
  /// Represents unknown changes sent to CloudKit
  public struct UnknownChanges {
  }

  public enum ModelChange {
    /// Pulled deletes from CloudKit
    case deletesPulled(Set<CloudKitIdentifier>)
    /// Pulled updates from CloudKit
    case updatesPulled(Set<Model>)
    
    /// Items deleted (on CloudKit) successfully
    case deletesPushed(Set<CloudKitIdentifier>)
    /// Items updated (on CloudKit) successfully
    case updatesPushed(Set<Model>)
    
    /// Deleted item IDs pushed to CloudKit that it does not recognize
    case unknownItemsDeleted(Set<CloudKitIdentifier>)
    /// Deleted items pushed to CloudKit that it does not recognize
    case unknownItemsPushed(Set<Model>)
  }

  // MARK: - Public Properties

  /// A publisher that sends a `ModelChanges` when models are updated or deleted on iCloud. No thread guarantees.
  public private(set) lazy var modelsChanged = modelsChangedSubject.eraseToAnyPublisher()
  
  /// Defaults to `.ifServerRecordUnchanged`
  public var savePolicy: CKModifyRecordsOperation.RecordSavePolicy = .ifServerRecordUnchanged
  
  /// The current iCloud account status for the user.
  @Published public internal(set) var accountStatus: AccountStatus = .unknown {
    willSet {
      // Setup the environment and force a sync if the user account status changes to available while the app is running
      if accountStatus != .unknown,
        newValue == .available
      {
        setupCloudEnvironment()
      }
    }
  }

  // MARK: - Internal Properties

  lazy var privateSubscriptionIdentifier = "\(zoneIdentifier.zoneName).subscription"
  lazy var privateChangeTokenKey = "TOKEN-\(zoneIdentifier.zoneName)"
  lazy var createdPrivateSubscriptionKey = "CREATEDSUBDB-\(zoneIdentifier.zoneName))"
  lazy var createdCustomZoneKey = "CREATEDZONE-\(zoneIdentifier.zoneName))"

  lazy var workQueue = DispatchQueue(
    label: "SyncEngine.Work.\(zoneIdentifier.zoneName)",
    qos: .userInitiated
  )
  private lazy var cloudQueue = DispatchQueue(
    label: "SyncEngine.Cloud.\(zoneIdentifier.zoneName)",
    qos: .userInitiated
  )

  let defaults: UserDefaults
  let recordType: CKRecord.RecordType
  let zoneIdentifier: CKRecordZone.ID

  let container: CKContainer
  let logHandler: (String, OSLogType) -> Void

  lazy var privateDatabase: CKDatabase = container.privateCloudDatabase

  var cancellables = Set<AnyCancellable>()
  let modelsChangedSubject = PassthroughSubject<ModelChanges, Never>()

  private lazy var uploadContext: UploadRecordContext<Model> = UploadRecordContext(
    defaults: defaults, zoneID: zoneIdentifier, logHandler: logHandler)
  private lazy var deleteContext: DeleteRecordContext<Model> = DeleteRecordContext(
    defaults: defaults, zoneID: zoneIdentifier, logHandler: logHandler)

  lazy var cloudOperationQueue: OperationQueue = {
    let queue = OperationQueue()

    queue.underlyingQueue = cloudQueue
    queue.name = "SyncEngine.Cloud.\(zoneIdentifier.zoneName))"
    queue.maxConcurrentOperationCount = 1

    return queue
  }()

  /// - Parameters:
  ///   - defaults: The `UserDefaults` used to store sync state information
  ///   - containerIdentifier: An optional bundle identifier of the app whose container you want to access. The bundle identifier must be in the app’s com.apple.developer.icloud-container-identifiers entitlement. If this value is nil, the default container object will be used.
  ///   - initialItems: An initial array of items to sync
  ///
  /// `initialItems` is used to perform a sync of any local models that don't yet exist in CloudKit. The engine uses the
  /// presence of data in `cloudKitSystemFields` to determine what models to upload. Alternatively, you can just call `upload(_:)` to sync initial items.
  public init(
    defaults: UserDefaults = .standard,
    containerIdentifier: String? = nil,
    initialItems: [Model] = [],
    logHandler: ((String, OSLogType) -> Void)? = nil
  ) {
    self.defaults = defaults
    self.recordType = String(describing: Model.self)
    let zoneIdent = CKRecordZone.ID(
      zoneName: self.recordType,
      ownerName: CKCurrentUserDefaultName
    )
    self.zoneIdentifier = zoneIdent
    if let containerIdentifier = containerIdentifier {
      self.container = CKContainer(identifier: containerIdentifier)
    } else {
      self.container = CKContainer.default()
    }

    self.logHandler =
      logHandler ?? { string, level in
        if #available(iOS 14.0, macOS 11.0, watchOS 7.0, tvOS 14.0, *) {
          let logger = Logger.init(
            subsystem: "com.jayhickey.Cirrus.\(zoneIdent)",
            category: String(describing: SyncEngine.self)
          )
          logger.log(level: level, "\(string)")
        }
      }

    // Add items that haven't been uploaded yet.
    self.uploadContext.buffer(initialItems.filter { $0.cloudKitSystemFields == nil })

    observeAccountStatus()
    setupCloudEnvironment()
  }

  // MARK: - Public Methods

  /// Upload models to CloudKit.
  public func upload(_ models: Model...) {
    upload(models)
  }

  /// Upload an array of models to CloudKit.
  /// - Parameters:
  ///   - models: models to upload
  ///   - waitUntilFinished: wait until finished
  public func upload(_ models: [Model], waitUntilFinished: Bool = false) {
    logHandler(#function, .debug)

    workQueue.async { [weak self] in
      guard let self else { return }
      
      self.uploadContext.buffer(models)
      self.modifyRecords(with: self.uploadContext)
      
      if waitUntilFinished {
        cloudOperationQueue.waitUntilAllOperationsAreFinished()
      }
    }
  }

  /// Delete models from CloudKit.
  public func delete(_ models: Model...) {
    delete(models)
  }

  /// Delete an array of models from CloudKit.
  /// - Parameters:
  ///   - models: models
  ///   - waitUntilFinished: wait until finished
  public func delete(_ models: [Model], waitUntilFinished: Bool = false) {
    logHandler(#function, .debug)

    workQueue.async { [weak self] in
      guard let self else { return }
      
      // Remove any pending upload items that match the items we want to delete
      self.uploadContext.removeFromBuffer(models)

      self.deleteContext.buffer(models)
      self.modifyRecords(with: self.deleteContext)
      
      if waitUntilFinished {
        cloudOperationQueue.waitUntilAllOperationsAreFinished()
      }
    }
  }
  
  /// Forces a data synchronization with CloudKit.
  /// 
  /// Use this method for force sync any data that may not have been able to upload
  /// to CloudKit automatically due to network conditions or other factors.
  /// 
  /// This method performs the following actions (in this order):
  /// 1. Uploads any models that were passed to `upload(_:)` and were unable to be uploaded to CloudKit.
  /// 2. Deletes any models that were passed to `delete(_:)` and were unable to be deleted from CloudKit.
  /// 3. Fetches any new model changes from CloudKit.
  /// - Parameter resettingToken: reset the sync token to fetch everything. Default: `false`
  public func forceSync(resettingToken: Bool = false) {
    logHandler(#function, .debug)

    // Fetch changes before pushing changes. This way we avoid pushing out udpates to remotely deleted
    // objects
    self.pullChanges(resettingToken: resettingToken)
    
    // Push local updates / deletes
    self.pushChanges()
  }
  
  /// Fetch remote changes
  /// - Parameter resettingToken: reset the sync token to fetch everything. Default: `false`
  /// - Parameter onCompletion: completion called after fetching remote changes. No thread guarantees.
  public func pullChanges(resettingToken: Bool = false, 
                          onCompletion: ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)? = nil) {
    logHandler(#function, .debug)
    
    workQueue.async { [weak self] in
      guard let self else { return }
      
      if resettingToken {
        self.privateChangeToken = nil
      }
      
      self.fetchRemoteChanges(onCompletion: onCompletion)
    }
  }
  
  public func pushChanges() {
    logHandler(#function, .debug)
    
    workQueue.async { [weak self] in
      guard let self else { return }
            
      // Push local updates / deletes
      self.performUpdate(with: self.uploadContext)
      self.performUpdate(with: self.deleteContext)
    }
  }
  
  /// Users of SyncEngine must call this method to update the server's change token after consuming `ModelChanges`
  /// so that the token is not inadvertently saved before changes have been processed. Power failure, other fatal crashes
  /// may prevent the caller from successfully consuming these changes.
  /// - Parameter changeToken: new change token
  public func updateChangeToken(_ changeToken: CKServerChangeToken?,
                                onCompletion: (() -> ())? = nil) {
    guard let changeToken else {
      self.logHandler("Ignored commiting empty change token", .info)
      
      return
    }
    
    workQueue.async { [weak self] in
      guard let self else { return }
      
      self.logHandler("Commiting new change token", .info)
      
      self.privateChangeToken = changeToken
      
      onCompletion?()
    }
  }

  /// Processes remote change push notifications from CloudKit.
  ///
  /// To subscribe to automatic changes, register for CloudKit push notifications by calling `application.registerForRemoteNotifications()`
  /// in your AppDelegate's `application(_:didFinishLaunchingWithOptions:)`. Then, call this method in
  /// `application(_:didReceiveRemoteNotification:fetchCompletionHandler:)` to process remote changes from CloudKit.
  /// - Parameters:
  ///   - userInfo: A dictionary that contains information about the remote notification. Pass the `userInfo` dictionary from `application(_:didReceiveRemoteNotification:fetchCompletionHandler:)` here.
  /// - Returns: Whether or not this notification was processed by the sync engine.
  @discardableResult public func processRemoteChangeNotification(with userInfo: [AnyHashable: Any]) -> Bool {
    logHandler(#function, .debug)

    guard let notification = CKNotification(fromRemoteNotificationDictionary: userInfo) else {
      return false
    }

    guard notification.subscriptionID == privateSubscriptionIdentifier else {
      logHandler("Not our subscription ID", .error)
      return false
    }

    logHandler("Received remote CloudKit notification for user data", .debug)

    self.workQueue.async { [weak self] in
      guard let self else { return }
      
      self.fetchRemoteChanges()
    }

    return true
  }

  // MARK: - Private Methods

  private func setupCloudEnvironment() {
    workQueue.async { [weak self] in
      guard let self else { return }

      // Initialize CloudKit with private custom zone, but bail early if we fail
      guard self.initializeZone(with: self.cloudOperationQueue) else {
        self.logHandler("Unable to initialize zone, bailing from setup early", .error)
        return
      }

      // Subscribe to CloudKit changes, but bail early if we fail
      guard self.initializeSubscription(with: self.cloudOperationQueue) else {
        self.logHandler(
          "Unable to initialize subscription to changes, bailing from setup early", .error)
        return
      }
      self.logHandler("Cloud environment preparation done", .debug)
    }
  }
}

// MARK: - Async Wrappers

extension SyncEngine {
  
  /// Async wrapper for `pullChanges` where remote changes are fetched and returned
  public func pullChanges(resettingToken: Bool = false) async throws -> SyncEngine<Model>.ModelChanges {
    try await withCheckedThrowingContinuation { continuation in
      self.pullChanges(resettingToken: resettingToken) { result in
        switch result {
          case .success(let changes):
            continuation.resume(returning: changes)
          case .failure(let error):
            continuation.resume(throwing: error)
        }
      }
    }
  }
  
  /// Upload a single record to CloudKit
  ///
  /// - Parameters:
  ///   - model: model to upload
  public func upload(_ model: Model) async throws -> SyncEngine<Model>.ModelChanges {
    try await withCheckedThrowingContinuation { continuation in
      self.saveRecord(model, usingContext: self.uploadContext) { result in
        switch result {
          case .success(let changes):
            continuation.resume(returning: changes)
          case .failure(let error):
            continuation.resume(throwing: error)
        }
      }
    }
  }
  
  /// Delete a single record from CloudKit
  ///
  /// - Parameters:
  ///   - model: model to delete
  public func delete(_ model: Model) async throws -> SyncEngine<Model>.ModelChanges {
    try await withCheckedThrowingContinuation { continuation in
      self.deleteRecord(model, usingContext: self.deleteContext) { result in
        switch result {
          case .success(let changes):
            continuation.resume(returning: changes)
          case .failure(let error):
            continuation.resume(throwing: error)
        }
      }
    }
  }
  
  /// Update the change token async
  /// - Parameter changeToken: change token
  public func updateChangeTokenAsync(_ changeToken: CKServerChangeToken?) async {
    await withCheckedContinuation { continuation in
      updateChangeToken(changeToken) {
        continuation.resume()
      }
    }
  }
}
