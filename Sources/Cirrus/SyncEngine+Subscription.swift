import CloudKit
import Foundation
import os.log

// NOTE: CloudKit will only allow a single subscription per device + zone combination etc
// Changing subscription keys between Debug and Production means the code has to re-create the
// subscription and delete the old one, which probably isn't worth doing. Thus, this uses
// the same key for both.

extension SyncEngine {

  // MARK: - Internal

  func initializeSubscription() async throws -> Bool {
    try await withCheckedThrowingContinuation { continuation in
      createPrivateSubscriptionsIfNeeded { result in
        switch result {
          case .success(let success):
            continuation.resume(returning: success)
          case .failure(let error):
            continuation.resume(throwing: error)
        }
      }
    }
  }
  
  // MARK: - Private

  private var createdPrivateSubscription: Bool {
    get {
      return defaults.bool(forKey: createdPrivateSubscriptionKey)
    }
    set {
      defaults.set(newValue, forKey: createdPrivateSubscriptionKey)
    }
  }

  private func createPrivateSubscriptionsIfNeeded(isRetrying: Bool = false, onCompletion: @escaping ((Result<Bool, Error>) -> Void)) {
    guard !createdPrivateSubscription else {
      logHandler(
        "Already subscribed to private database changes, skipping subscription but checking if it really exists",
        .debug)

      checkSubscription(isRetrying: isRetrying, onCompletion: onCompletion)

      return
    }

    let subscription = CKRecordZoneSubscription(zoneID: zoneIdentifier, subscriptionID: privateSubscriptionIdentifier)

    let notificationInfo = CKSubscription.NotificationInfo()
    notificationInfo.shouldSendContentAvailable = true

    subscription.notificationInfo = notificationInfo
    subscription.recordType = recordType

    let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: nil)

    operation.database = privateDatabase

    operation.modifySubscriptionsCompletionBlock = { [weak self] _, _, error in
      guard let self else { return }

      if let error {
        self.logHandler(
          "Failed to create private CloudKit subscription: \(String(describing: error))", .error)

        var result = false
        
        // Don't try a second time
        if !isRetrying {
          result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.createPrivateSubscriptionsIfNeeded(isRetrying: true, onCompletion: onCompletion)
          }
        }
        
        if result == false {
          onCompletion(.failure(error))
        }
      } else {
        self.logHandler("Private subscription created successfully", .info)
        self.createdPrivateSubscription = true
        
        onCompletion(.success(true))
      }
    }

    cloudOperationQueue.addOperation(operation)
    
    // Wait for operation to complete
    cloudOperationQueue.waitUntilAllOperationsAreFinished()
  }

  private func checkSubscription(isRetrying: Bool = false, onCompletion: @escaping ((Result<Bool, Error>) -> Void)) {
    let operation = CKFetchSubscriptionsOperation(subscriptionIDs: [privateSubscriptionIdentifier])

    operation.fetchSubscriptionCompletionBlock = { [weak self] ids, error in
      guard let self else { return }

      if let error {
        if let ckError = error as? CKError, ckError.code == .unknownItem || ckError.code == .partialFailure {
          self.logHandler("Subscription not found, will try and re-create: \(String(describing: error))",.error)
          
          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            self.createdPrivateSubscription = false
            self.createPrivateSubscriptionsIfNeeded(onCompletion: onCompletion)
          }
        }
        else {
          self.logHandler("Failed to check for private zone subscription existence: \(String(describing: error))",.error)

          var result = false
          
          if !isRetrying {
            result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
              self.checkSubscription(onCompletion: onCompletion)
            }
          }
          
          if isRetrying || result == false {
            self.logHandler("Irrecoverable error when fetching private zone subscription, assuming it doesn't exist: \(String(describing: error))", .error)
            
            self.workQueue.async { [weak self] in
              guard let self else { return }
              
              self.createdPrivateSubscription = false
              self.createPrivateSubscriptionsIfNeeded(onCompletion: onCompletion)
            }
          }
        }
      } else if ids?.isEmpty ?? true {
        self.logHandler("Private subscription reported as existing, but it doesn't exist. Creating.", .error)

        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          self.createdPrivateSubscription = false
          self.createPrivateSubscriptionsIfNeeded(onCompletion: onCompletion)
        }
      } else {
        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          self.logHandler("Private subscription found, the device is subscribed to CloudKit change notifications.", .info)
          
          // If checking failed earlier, we set this flag to false, must reset to true if subscription in fact exists, otherwise we'll keep
          // trying to create the private subscription, which will always throw an error
          if self.createdPrivateSubscription == false {
            self.createdPrivateSubscription = true
            
            self.logHandler("Private subscription exists, updating flag", .debug)
          }
          
          onCompletion(.success(true))
        }
      }
    }

    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
  }
}
