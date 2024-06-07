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
  
  /// Defaults to `.ifServerRecordUnchanged`
  public var savePolicy: CKModifyRecordsOperation.RecordSavePolicy = .ifServerRecordUnchanged
  
  /// The current iCloud account status for the user. Observe on this and call `setup()` when this changes
  @Published public internal(set) var accountStatus: AccountStatus = .unknown
  
  // MARK: - Internal Properties
  
  // Subscriptions are one per device - must be the same for both dev and production
  lazy var privateSubscriptionIdentifier = "\(zoneIdentifier.zoneName).subscription"
  
  /// UserDefaults key for storing change-token. Different for dev / production to validate / test
  lazy var privateChangeTokenKey = "TOKEN-\(zoneIdentifier.zoneName)"
  
  // Note: this has a trailing ) but too late to fix after it's been deployed
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
  
  /// Flag to check if user has setup the environment before usage
  var hasSetupEnv = false
  
  let defaults: UserDefaults
  let recordType: CKRecord.RecordType
  let zoneIdentifier: CKRecordZone.ID
  
  let container: CKContainer
  let logHandler: (String, OSLogType) -> Void
  
  lazy var privateDatabase: CKDatabase = container.privateCloudDatabase
  
  var cancellables = Set<AnyCancellable>()
  
  private lazy var uploadContext: UploadRecordContext<Model> = UploadRecordContext(
    defaults: defaults, zoneID: zoneIdentifier, logHandler: logHandler)
  private lazy var deleteContext: DeleteRecordContext<Model> = DeleteRecordContext(
    defaults: defaults, zoneID: zoneIdentifier, logHandler: logHandler)
  
  lazy var cloudOperationQueue: OperationQueue = {
    let queue = OperationQueue()
    
    queue.underlyingQueue = cloudQueue
    queue.name = "SyncEngine.Cloud.\(zoneIdentifier.zoneName)"
    queue.maxConcurrentOperationCount = 1
    
    return queue
  }()
  
  /// Initializes the cloudkit container, however you must call the `setup()` method separately to setup the cloud kit environment
  ///
  /// - Parameters:
  ///   - defaults: The `UserDefaults` used to store sync state information
  ///   - containerIdentifier: An optional bundle identifier of the app whose container you want to access. The bundle identifier must be in the app’s com.apple.developer.icloud-container-identifiers entitlement. If this value is nil, the default container object will be used.
  public init(
    defaults: UserDefaults = .standard,
    containerIdentifier: String? = nil,
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
    
    self.logHandler = logHandler ?? { string, level in
      if #available(iOS 14.0, macOS 11.0, watchOS 7.0, tvOS 14.0, *) {
        let logger = Logger.init(
          subsystem: "com.jayhickey.Cirrus.\(zoneIdent)",
          category: String(describing: SyncEngine.self)
        )
        logger.log(level: level, "\(string)")
      }
    }
    
    NotificationCenter.default.publisher(for: .CKAccountChanged, object: nil).sink {
      [weak self] _ in
      
      self?.logHandler("CloudKit account status changed - resetting publisher", .info)
      
      self?.hasSetupEnv = false
      
      // Account status changed, unset status so we can re-check
      self?.accountStatus = .unknown
    }
    .store(in: &cancellables)
  }
  
  deinit {
    cancellables.removeAll()
  }
  
  // MARK: - Public Methods
  
  /// Setup CloudKit environment and check for account statis
  /// - Returns: returns `false` if CloudKit is not available and so sync cannot happen
  public func setup() async throws -> Bool {
    guard !hasSetupEnv else {
      return true
    }
    
    let accountStatus = try await observeAccountStatus()
    
    if accountStatus != .available {
      let accStr: String
      switch accountStatus {
        case .unknown:
          accStr = "Unknown"
        case .couldNotDetermine:
          accStr = "Could not determine"
        case .available:
          accStr = "Available"
        case .restricted:
          accStr = "Restricted"
        case .noAccount:
          accStr = "No account"
      }
      logHandler("CloudKit account is not available, skipping setup: \(accStr)", .info)
      
      // We've setup
      hasSetupEnv = true
      
      return false
    }
    
    let didSetup = try await setupCloudEnvironment()
    
    hasSetupEnv = true
    
    return didSetup
  }
  
  /// Fetch remote changes
  ///
  /// - SeeAlso `updateChangeToken()` which must be called, passing the change token returned by this call
  ///
  /// - Parameter resettingToken: reset the token before performing fetch if desired
  public func pullChanges(resettingToken: Bool = false) async throws -> SyncEngine<Model>.ModelChanges {
    assert(hasSetupEnv, "Must call setup() first")
    
    return try await withCheckedThrowingContinuation { continuation in
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
  
  /// Update the change token after the caller has processed changes from `pullChanges()`
  ///
  /// - Parameter changeToken: change token
  public func updateChangeToken(_ changeToken: CKServerChangeToken?) async {
    await withCheckedContinuation { continuation in
      setChangeToken(changeToken) {
        continuation.resume()
      }
    }
  }
  
  /// Check if this is a CloudKit notification that our sync engine can process.
  ///
  /// To subscribe to automatic changes, register for CloudKit push notifications by calling `application.registerForRemoteNotifications()`
  /// in your AppDelegate's `application(_:didFinishLaunchingWithOptions:)`. Then, call this method in
  /// `application(_:didReceiveRemoteNotification:fetchCompletionHandler:)` to process remote changes from CloudKit.
  /// - Parameters:
  ///   - userInfo: A dictionary that contains information about the remote notification. Pass the `userInfo` dictionary from `application(_:didReceiveRemoteNotification:fetchCompletionHandler:)` here.
  /// - Returns: Whether or not this notification was processed by the sync engine.
  @discardableResult public func canProcessRemoteChangeNotification(with userInfo: [AnyHashable: Any]) -> Bool {
    logHandler(#function, .debug)
    
    guard let notification = CKNotification(fromRemoteNotificationDictionary: userInfo) else {
      return false
    }
    
    guard notification.subscriptionID == privateSubscriptionIdentifier else {
      logHandler("Not our subscription ID", .error)
      return false
    }
    
    logHandler("Received remote CloudKit notification for user data", .debug)
    
    return true
  }
  
  // MARK: - Private Methods
  
  private func setupCloudEnvironment() async throws -> Bool {
    // Initialize CloudKit with private custom zone, but bail early if we fail
    if try await self.initializeZone() == false {
      self.logHandler("Unable to initialize zone, bailing from setup early", .error)
      return false
    }
    
    // Subscribe to CloudKit changes, but bail early if we fail
    if try await self.initializeSubscription() == false {
      self.logHandler("Unable to initialize subscription to changes, bailing from setup early", .error)
      return false
    }
    
    self.logHandler("Cloud environment preparation done", .debug)
    
    return true
  }
  
  /// Fetch remote changes
  /// - Parameter resettingToken: reset the sync token to fetch everything. Default: `false`
  /// - Parameter onCompletion: completion called after fetching remote changes. No thread guarantees.
  private func pullChanges(resettingToken: Bool = false,
                           onCompletion: @escaping ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)) {
    logHandler(#function, .debug)
    
    workQueue.async { [weak self] in
      guard let self else { return }
      
      if resettingToken {
        self.privateChangeToken = nil
      }
      
      self.fetchRemoteChanges(onCompletion: onCompletion)
    }
  }
  
  /// Users of SyncEngine must call this method to update the server's change token after consuming `ModelChanges`
  /// so that the token is not inadvertently saved before changes have been processed. Power failure, other fatal crashes
  /// may prevent the caller from successfully consuming these changes.
  /// - Parameter changeToken: new change token
  private func setChangeToken(_ changeToken: CKServerChangeToken?,
                              onCompletion: @escaping (() -> ())) {
    guard let changeToken else {
      self.logHandler("Ignored commiting empty change token", .info)
      
      return
    }
    
    workQueue.async { [weak self] in
      guard let self else { return }
      
      self.logHandler("Commiting new change token", .info)
      
      self.privateChangeToken = changeToken
      
      onCompletion()
    }
  }
  
}
