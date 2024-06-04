import CloudKit
import Foundation
import os.log

extension SyncEngine {

  // MARK: - Internal

  func initializeSubscription(with queue: OperationQueue) -> Bool {
    self.createPrivateSubscriptionsIfNeeded()
    queue.waitUntilAllOperationsAreFinished()
    guard self.createdPrivateSubscription else { return false }
    return true
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

  private func createPrivateSubscriptionsIfNeeded() {
    guard !createdPrivateSubscription else {
      logHandler(
        "Already subscribed to private database changes, skipping subscription but checking if it really exists",
        .debug)

      checkSubscription()

      return
    }

    let subscription = CKRecordZoneSubscription(
      zoneID: zoneIdentifier, subscriptionID: privateSubscriptionIdentifier)

    let notificationInfo = CKSubscription.NotificationInfo()
    notificationInfo.shouldSendContentAvailable = true

    subscription.notificationInfo = notificationInfo
    subscription.recordType = recordType

    let operation = CKModifySubscriptionsOperation(
      subscriptionsToSave: [subscription], subscriptionIDsToDelete: nil)

    operation.database = privateDatabase
    operation.qualityOfService = .userInitiated

    operation.modifySubscriptionsCompletionBlock = { [weak self] _, _, error in
      guard let self = self else { return }

      if let error = error {
        self.logHandler(
          "Failed to create private CloudKit subscription: \(String(describing: error))", .error)

        error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
          self.createPrivateSubscriptionsIfNeeded()
        }
      } else {
        self.logHandler("Private subscription created successfully", .info)
        self.createdPrivateSubscription = true
      }
    }

    cloudOperationQueue.addOperation(operation)
  }

  private func checkSubscription() {
    let operation = CKFetchSubscriptionsOperation(subscriptionIDs: [privateSubscriptionIdentifier])

    operation.fetchSubscriptionCompletionBlock = { [weak self] ids, error in
      guard let self else { return }

      if let error = error {
        self.logHandler(
          "Failed to check for private zone subscription existence: \(String(describing: error))",
          .error)

        if !error.retryCloudKitOperationIfPossible(
          self.logHandler, queue: self.workQueue, with: { self.checkSubscription() })
        {
          self.logHandler(
            "Irrecoverable error when fetching private zone subscription, assuming it doesn't exist: \(String(describing: error))",
            .error)

          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            self.createdPrivateSubscription = false
            self.createPrivateSubscriptionsIfNeeded()
          }
        }
      } else if ids?.isEmpty ?? true {
        self.logHandler(
          "Private subscription reported as existing, but it doesn't exist. Creating.", .error
        )

        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          self.createdPrivateSubscription = false
          self.createPrivateSubscriptionsIfNeeded()
        }
      } else {
        self.logHandler(
          "Private subscription found, the device is subscribed to CloudKit change notifications.",
          .info
        )
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
  }
}
